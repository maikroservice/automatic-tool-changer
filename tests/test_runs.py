"""Async tests for background run execution — confluence_exporter."""
import copy
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import main
from main import run_in_background
from database import engine
import database as db

CREDS  = {"url": "https://company.atlassian.net/wiki", "email": "user@co.com", "api_token": "TOK", "auth_type": "basic"}
PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md", "output_dir": "./out"}


async def _seed(campaign_name="C", token_value=None):
    """Create a campaign + token in the DB; return (campaign, token)."""
    async with engine.begin() as conn:
        import uuid
        cid = str(uuid.uuid4())[:8]
        campaign = await db.db_create_campaign(
            conn, id=cid, name=campaign_name,
            webhook_url=None, webhook_secret=None, webhook_auth_header=None, created_by=None,
        )
        await db.db_activate_campaign(conn, cid)

        tid = str(uuid.uuid4())[:8]
        token = await db.db_create_token(
            conn, id=tid, campaign_id=cid,
            name="creds", value=token_value or CREDS,
            type="credential_object", created_by=None, meta={},
        )
    return campaign, token


async def _seed_run(campaign_id, token_id, tool_id="confluence_exporter", params=None):
    async with engine.begin() as conn:
        import uuid
        rid = str(uuid.uuid4())[:8]
        run = await db.db_create_run(
            conn, id=rid, campaign_id=campaign_id,
            tool_id=tool_id,
            tool_name=main.TOOLS.get(tool_id, {}).get("name", tool_id),
            token_ids=[token_id], token_names=["creds"],
            parameters=params or PARAMS,
            triggered_by=None, created_by=None,
        )
    return run


# ── lifecycle ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_transitions_to_running_then_completed():
    campaign, token = await _seed()
    run = await _seed_run(campaign["id"], token["id"])

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["status"] == "completed"
    assert updated["started_at"] is not None
    assert updated["completed_at"] is not None


@pytest.mark.asyncio
async def test_run_result_contains_cli_command():
    campaign, token = await _seed()
    run = await _seed_run(campaign["id"], token["id"])

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["status"] == "completed"
    assert isinstance(updated["result"], list)
    cmd = updated["result"][0]
    assert "confluence-exporter" in cmd
    assert "--space DEV" in cmd
    assert "--format md" in cmd


@pytest.mark.asyncio
async def test_run_result_contains_credentials():
    campaign, token = await _seed()
    run = await _seed_run(campaign["id"], token["id"])

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    cmd = updated["result"][0]
    assert "CONFLUENCE_URL=https://company.atlassian.net/wiki" in cmd
    assert "CONFLUENCE_EMAIL=user@co.com" in cmd
    assert "CONFLUENCE_TOKEN=TOK" in cmd


@pytest.mark.asyncio
async def test_run_with_multiple_credential_tokens():
    creds2 = {**CREDS, "url": "https://other.atlassian.net/wiki"}
    campaign, token1 = await _seed()
    async with engine.begin() as conn:
        import uuid
        tid2 = str(uuid.uuid4())[:8]
        token2 = await db.db_create_token(
            conn, id=tid2, campaign_id=campaign["id"],
            name="creds2", value=creds2,
            type="credential_object", created_by=None, meta={},
        )
        rid = str(uuid.uuid4())[:8]
        run = await db.db_create_run(
            conn, id=rid, campaign_id=campaign["id"],
            tool_id="confluence_exporter", tool_name="Confluence Exporter",
            token_ids=[token1["id"], token2["id"]], token_names=["creds", "creds2"],
            parameters=PARAMS, triggered_by=None, created_by=None,
        )

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["status"] == "completed"
    assert len(updated["result"]) == 2


@pytest.mark.asyncio
async def test_run_fails_on_invalid_tool():
    campaign, token = await _seed()
    async with engine.begin() as conn:
        import uuid
        rid = str(uuid.uuid4())[:8]
        run = await db.db_create_run(
            conn, id=rid, campaign_id=campaign["id"],
            tool_id="nonexistent_tool", tool_name="nonexistent_tool",
            token_ids=[token["id"]], token_names=["creds"],
            parameters={}, triggered_by=None, created_by=None,
        )

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["status"] == "failed"
    assert "Unknown tool" in updated["error"]


@pytest.mark.asyncio
async def test_run_sets_timestamps():
    campaign, token = await _seed()
    run = await _seed_run(campaign["id"], token["id"])

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["completed_at"] is not None
    datetime.fromisoformat(updated["completed_at"])


@pytest.mark.asyncio
async def test_run_skips_missing_tokens_gracefully():
    campaign, token = await _seed()
    async with engine.begin() as conn:
        import uuid
        rid = str(uuid.uuid4())[:8]
        run = await db.db_create_run(
            conn, id=rid, campaign_id=campaign["id"],
            tool_id="confluence_exporter", tool_name="Confluence Exporter",
            token_ids=[token["id"], "t_deleted"], token_names=["creds", "gone"],
            parameters=PARAMS, triggered_by=None, created_by=None,
        )

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    async with engine.begin() as conn:
        updated = await db.db_get_run(conn, run["id"])
    assert updated["status"] == "completed"
    assert len(updated["result"]) == 1


@pytest.mark.asyncio
async def test_run_does_nothing_for_unknown_run_id():
    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background("no-such-run")  # must not raise


@pytest.mark.asyncio
async def test_run_broadcasts_status_updates(monkeypatch):
    campaign, token = await _seed()
    run = await _seed_run(campaign["id"], token["id"])

    broadcasts = []

    async def fake_broadcast(msg):
        broadcasts.append(copy.deepcopy(msg))

    monkeypatch.setattr(main, "broadcast", fake_broadcast)

    with patch("main.asyncio.sleep", AsyncMock(return_value=None)):
        await run_in_background(run["id"])

    types = [b["type"] for b in broadcasts]
    assert "run_updated" in types
    statuses = [b["run"]["status"] for b in broadcasts if b["type"] == "run_updated"]
    assert statuses[0] == "running"
    assert statuses[-1] == "completed"
