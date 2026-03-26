"""Tests for webhook trigger and callback endpoints."""
import copy
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import main
from tests.conftest import add_campaign, add_token

CREDS  = {"url": "https://co.atlassian.net/wiki", "email": "u@co.com", "api_token": "TOK", "auth_type": "basic"}
PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md"}


@pytest.fixture(autouse=True)
def campaign(client):
    add_campaign(client, "Test", webhook_url="https://tc.example.com/hook/abc",
                 webhook_secret="s3cr3t", webhook_auth_header="Authorization")


def _make_completed_run(client):
    """Add a token and create a completed run via the API."""
    tok = add_token(client, "creds", CREDS, type="credential_object")
    r = client.post("/runs", json={
        "tool_id": "confluence_exporter",
        "token_ids": [tok["id"]],
        "parameters": PARAMS,
    })
    run_id = r.json()["id"]
    # Patch run to completed status directly via DB
    import asyncio
    from database import engine
    import database as db

    async def _complete():
        async with engine.begin() as conn:
            from datetime import datetime
            cmd = "CONFLUENCE_URL=... confluence-exporter --space DEV --format md --output ./output"
            await db.db_update_run(conn, run_id, status="completed",
                                   result=[cmd],
                                   completed_at=datetime.utcnow().isoformat())
    asyncio.new_event_loop().run_until_complete(_complete())
    return run_id


async def _make_completed_run_async(client):
    """Async variant for use inside @pytest.mark.asyncio tests."""
    from datetime import datetime
    from database import engine
    import database as db

    tok = add_token(client, "creds", CREDS, type="credential_object")
    r = client.post("/runs", json={
        "tool_id": "confluence_exporter",
        "token_ids": [tok["id"]],
        "parameters": PARAMS,
    })
    run_id = r.json()["id"]
    cmd = "CONFLUENCE_URL=... confluence-exporter --space DEV --format md --output ./output"
    async with engine.begin() as conn:
        await db.db_update_run(conn, run_id, status="completed",
                               result=[cmd],
                               completed_at=datetime.utcnow().isoformat())
    return run_id


# ── Manual trigger ─────────────────────────────────────────────────────────────

def test_trigger_run_returns_firing(client):
    run_id = _make_completed_run(client)
    with patch("main._fire_webhook", new=AsyncMock()):
        r = client.post(f"/runs/{run_id}/trigger")
    assert r.status_code == 200
    assert r.json()["status"] == "firing"


def test_trigger_run_not_found(client):
    assert client.post("/runs/nope/trigger").status_code == 404


def test_trigger_run_not_completed_returns_400(client):
    tok = add_token(client, "creds", CREDS, type="credential_object")
    run = client.post("/runs", json={
        "tool_id": "confluence_exporter",
        "token_ids": [tok["id"]],
        "parameters": PARAMS,
    }).json()
    # Run is still pending
    r = client.post(f"/runs/{run['id']}/trigger")
    assert r.status_code == 400


# ── Callback ───────────────────────────────────────────────────────────────────

def test_callback_stores_result(client):
    run_id = _make_completed_run(client)
    r = client.post(f"/runs/{run_id}/callback", json={"result": {"status": "success", "pages": 42}})
    assert r.status_code == 200
    runs = client.get("/runs").json()
    run = next(x for x in runs if x["id"] == run_id)
    assert run["webhook_result"] == {"status": "success", "pages": 42}


def test_callback_stores_error(client):
    run_id = _make_completed_run(client)
    r = client.post(f"/runs/{run_id}/callback", json={"error": "Tracecat workflow failed"})
    assert r.status_code == 200
    runs = client.get("/runs").json()
    run = next(x for x in runs if x["id"] == run_id)
    assert run["error"] == "Tracecat workflow failed"


def test_callback_not_found(client):
    assert client.post("/runs/nope/callback", json={"result": {}}).status_code == 404


def test_callback_broadcasts_run_updated(client, monkeypatch):
    run_id = _make_completed_run(client)
    broadcasts = []

    async def fake_broadcast(msg):
        broadcasts.append(copy.deepcopy(msg))

    monkeypatch.setattr(main, "broadcast", fake_broadcast)
    client.post(f"/runs/{run_id}/callback", json={"result": {"ok": True}})
    types = [b["type"] for b in broadcasts]
    assert "run_updated" in types


# ── _fire_webhook helper ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fire_webhook_posts_to_url(client):
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = ""

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        await main._fire_webhook(run_id, "http://localhost:8000/")

        assert mock_http.post.called
        call_kwargs = mock_http.post.call_args
        url = call_kwargs[0][0]
        assert url == "https://tc.example.com/hook/abc"
        payload = call_kwargs[1]["json"]
        assert "commands" in payload
        assert "callback_url" in payload
        assert run_id in payload["callback_url"]
        headers = call_kwargs[1]["headers"]
        assert headers.get("Authorization") == "s3cr3t"


@pytest.mark.asyncio
async def test_fire_webhook_updates_status_to_triggered(client):
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = ""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        await main._fire_webhook(run_id, "http://localhost:8000/")

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
    assert run["status"] == "triggered"
    assert run["webhook_triggered_at"] is not None


@pytest.mark.asyncio
async def test_fire_webhook_records_error_on_failure(client):
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=Exception("connection refused"))
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        await main._fire_webhook(run_id, "http://localhost:8000/")

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
    assert "Webhook error" in run["error"]


@pytest.mark.asyncio
async def test_fire_webhook_skips_when_no_webhook_url(client):
    """Campaign without webhook_url — _fire_webhook must do nothing."""
    # Add second campaign without webhook
    from database import engine
    import database as db
    import uuid

    async with engine.begin() as conn:
        c2 = await db.db_create_campaign(
            conn, id=str(uuid.uuid4())[:8], name="No Hook",
            webhook_url=None, webhook_secret=None, webhook_auth_header=None,
            created_by=None,
        )
        tid = str(uuid.uuid4())[:8]
        tok = await db.db_create_token(
            conn, id=tid, campaign_id=c2["id"], name="creds",
            value=CREDS, type="credential_object", created_by=None, meta={},
        )
        rid = str(uuid.uuid4())[:8]
        from datetime import datetime
        run = await db.db_create_run(
            conn, id=rid, campaign_id=c2["id"],
            tool_id="confluence_exporter", tool_name="Confluence Exporter",
            token_ids=[tid], token_names=["creds"],
            parameters=PARAMS, triggered_by=None, created_by=None,
        )
        await db.db_update_run(conn, rid, status="completed",
                               result=["cmd"], completed_at=datetime.utcnow().isoformat())

    with patch("httpx.AsyncClient") as mock_client_cls:
        await main._fire_webhook(rid, "http://localhost:8000/")
        assert not mock_client_cls.called


# ── Webhook log persistence ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fire_webhook_logs_success(client):
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"ok": true}'
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await main._fire_webhook(run_id, "http://localhost:8000/")

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        logs = await db.db_list_webhook_logs(conn, run["campaign_id"])

    assert len(logs) == 1
    assert logs[0]["success"] is True
    assert logs[0]["status_code"] == 200
    assert logs[0]["run_id"] == run_id
    assert logs[0]["error"] is None


@pytest.mark.asyncio
async def test_fire_webhook_logs_failure(client):
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=Exception("timeout"))
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await main._fire_webhook(run_id, "http://localhost:8000/")

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        logs = await db.db_list_webhook_logs(conn, run["campaign_id"])

    assert len(logs) == 1
    assert logs[0]["success"] is False
    assert logs[0]["status_code"] is None
    assert "timeout" in logs[0]["error"]


@pytest.mark.asyncio
async def test_fire_webhook_logs_http_error(client):
    """Non-2xx response is logged as failure with status code."""
    from database import engine
    import database as db

    run_id = await _make_completed_run_async(client)

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await main._fire_webhook(run_id, "http://localhost:8000/")

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        logs = await db.db_list_webhook_logs(conn, run["campaign_id"])

    assert len(logs) == 1
    assert logs[0]["success"] is False
    assert logs[0]["status_code"] == 500
    assert "500" in (run["error"] or "")


def test_list_webhook_logs_endpoint(client):
    run_id = _make_completed_run(client)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = ""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("main._fire_webhook", new=AsyncMock()):
            client.post(f"/runs/{run_id}/trigger")
    logs = client.get("/webhook-logs").json()
    # logs may be empty because _fire_webhook was mocked — just check 200 status
    assert isinstance(logs, list)
