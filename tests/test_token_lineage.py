"""Tests for /tokens/{id}/runs endpoint (token run lineage)."""
import asyncio
import pytest

import main
import database as db
from database import engine
from tests.conftest import add_campaign

APP_TOKEN = main.APP_TOKEN
CREDS = {"url": "https://co.atlassian.net", "email": "u@co.com", "api_token": "TOK"}


@pytest.fixture(autouse=True)
def campaign(client):
    return add_campaign(client, "Test Campaign")


def _gen_key(client, campaign_id):
    r = client.post(f"/campaigns/{campaign_id}/ingest-keys",
                    json={"name": "test-key"}, headers={"X-ATC-Token": APP_TOKEN})
    assert r.status_code == 200
    return r.json()


def _ingest(client, key):
    return client.post("/ingest",
                       json={"source": "nophish", "credentials": CREDS},
                       headers={"X-Ingest-Key": key})


def _create_run(campaign_id, token_id, token_name, run_id="run001"):
    async def _inner():
        async with engine.begin() as conn:
            return await db.db_create_run(
                conn, id=run_id, campaign_id=campaign_id,
                tool_id="confluence_exporter", tool_name="Confluence Exporter",
                token_ids=[token_id], token_names=[token_name],
                parameters={}, triggered_by=None, created_by=None,
            )
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_inner())
    loop.close()
    return result


def test_db_list_runs_by_token_returns_empty_initially(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    token_id = _ingest(client, key).json()["id"]
    runs = client.get(f"/tokens/{token_id}/runs").json()
    assert runs == []


def test_db_list_runs_by_token_returns_matching_runs(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    token = _ingest(client, key).json()
    token_id = token["id"]
    _create_run(campaign["id"], token_id, token["name"], run_id="run001")

    runs = client.get(f"/tokens/{token_id}/runs").json()
    assert len(runs) == 1
    assert runs[0]["id"] == "run001"


def test_db_list_runs_by_token_excludes_other_tokens(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    token_a = _ingest(client, key).json()
    token_b = _ingest(client, key).json()
    _create_run(campaign["id"], token_b["id"], token_b["name"], run_id="runB")

    runs = client.get(f"/tokens/{token_a['id']}/runs").json()
    assert runs == []


def test_token_runs_endpoint_returns_correct_runs(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    token = _ingest(client, key).json()
    token_id = token["id"]
    _create_run(campaign["id"], token_id, token["name"], run_id="run1")

    async def _create_other():
        async with engine.begin() as conn:
            await db.db_create_run(
                conn, id="run2", campaign_id=campaign["id"],
                tool_id="confluence_exporter", tool_name="Confluence Exporter",
                token_ids=["other_token"], token_names=["other"],
                parameters={}, triggered_by=None, created_by=None,
            )
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_create_other())
    loop.close()

    runs = client.get(f"/tokens/{token_id}/runs").json()
    assert len(runs) == 1
    assert runs[0]["id"] == "run1"
