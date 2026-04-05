"""Tests for the immutable audit log."""
import asyncio
import pytest
from fastapi.testclient import TestClient

import main
import database as db
from database import engine
from tests.conftest import add_campaign, add_token

APP_TOKEN = main.APP_TOKEN
CREDS = {"url": "https://co.atlassian.net", "email": "u@co.com", "api_token": "TOK"}


@pytest.fixture(autouse=True)
def campaign(client):
    return add_campaign(client, "Test Campaign")


def _auth():
    return {"X-ATC-Token": APP_TOKEN}


def _gen_key(client, campaign_id, name="audit-key"):
    r = client.post(f"/campaigns/{campaign_id}/ingest-keys",
                    json={"name": name}, headers=_auth())
    assert r.status_code == 200
    return r.json()


def _get_audit(campaign_id):
    async def _inner():
        async with engine.begin() as conn:
            return await db.db_list_audit(conn, campaign_id)
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_inner())
    loop.close()
    return result


# ── Append-only contract ───────────────────────────────────────────────────────

def test_audit_log_append_only():
    """db_append_audit exists; db_delete_audit and db_update_audit must NOT exist."""
    assert hasattr(db, "db_append_audit")
    assert not hasattr(db, "db_delete_audit")
    assert not hasattr(db, "db_update_audit")


# ── Token events ───────────────────────────────────────────────────────────────

def test_add_token_creates_audit_entry(client, campaign):
    add_token(client, "mycreds", CREDS, type="credential_object")
    entries = _get_audit(campaign["id"])
    token_entries = [e for e in entries if e["action"] == "token.created"]
    assert len(token_entries) == 1
    assert token_entries[0]["actor"] == "operator"
    assert token_entries[0]["entity_type"] == "token"


def test_ingest_creates_audit_entry_with_ingest_key_actor(client, campaign):
    key_body = _gen_key(client, campaign["id"], name="my-key")
    client.post("/ingest",
                json={"source": "nophish", "credentials": CREDS},
                headers={"X-Ingest-Key": key_body["key"]})
    entries = _get_audit(campaign["id"])
    ingest_entries = [e for e in entries
                      if e["action"] == "token.created" and e["actor"] != "operator"]
    assert len(ingest_entries) == 1
    assert ingest_entries[0]["actor"] == "ingest_key:my-key"


# ── Run events ─────────────────────────────────────────────────────────────────

def test_token_created_operator_detail_includes_value(client, campaign):
    """Operator-created token audit must snapshot value; id/campaign_id must not be in detail."""
    add_token(client, "mycreds", CREDS, type="credential_object")
    entries = _get_audit(campaign["id"])
    e = next(x for x in entries
             if x["action"] == "token.created" and x["actor"] == "operator")
    detail = e["detail"]
    assert detail["value"] == CREDS
    assert detail["name"] == "mycreds"
    assert detail["type"] == "credential_object"
    assert "id" not in detail
    assert "campaign_id" not in detail


def test_token_created_ingest_detail_includes_credentials(client, campaign):
    """Ingested token audit must snapshot credentials; id/campaign_id must not be in detail."""
    key = _gen_key(client, campaign["id"])["key"]
    client.post("/ingest",
                json={"source": "nophish", "credentials": CREDS},
                headers={"X-Ingest-Key": key})
    entries = _get_audit(campaign["id"])
    e = next(x for x in entries
             if x["action"] == "token.created" and x["actor"] != "operator")
    detail = e["detail"]
    assert detail["value"] == CREDS
    assert detail["meta"]["source"] == "nophish"
    assert "id" not in detail
    assert "campaign_id" not in detail


def test_run_created_manually_creates_audit_entry(client, campaign):
    tok = add_token(client, "creds", CREDS, type="credential_object")
    client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [tok["id"]], "parameters": {}})
    entries = _get_audit(campaign["id"])
    run_entries = [e for e in entries if e["action"] == "run.created" and e["actor"] == "operator"]
    assert len(run_entries) == 1


def test_run_created_detail_includes_token_ids_and_parameters(client, campaign):
    """Run audit detail must snapshot the full run record without id/campaign_id duplication."""
    tok = add_token(client, "creds", CREDS, type="credential_object")
    params = {"scope": "space", "scope_value": "DEV"}
    client.post("/runs", json={"tool_id": "confluence_exporter",
                               "token_ids": [tok["id"]], "parameters": params})
    entries = _get_audit(campaign["id"])
    e = next(x for x in entries if x["action"] == "run.created" and x["actor"] == "operator")
    detail = e["detail"]
    assert tok["id"] in detail["token_ids"]
    assert detail["parameters"] == params
    assert detail["tool_id"] == "confluence_exporter"
    assert "id" not in detail
    assert "campaign_id" not in detail


def test_run_auto_triggered_creates_audit_entry_with_watcher_actor(client, campaign):
    client.post("/watchers", json={"tool_id": "confluence_exporter",
                                   "token_type": "credential_object", "name": "W1"})
    watchers = client.get("/watchers").json()
    watcher_id = watchers[0]["id"]
    add_token(client, "creds", CREDS, type="credential_object")
    entries = _get_audit(campaign["id"])
    auto_entries = [e for e in entries
                    if e["action"] == "run.created" and e["actor"] == f"watcher:{watcher_id}"]
    assert len(auto_entries) == 1


# ── Campaign events ───────────────────────────────────────────────────────────

def test_campaign_created_creates_audit_entry(client, campaign):
    entries = _get_audit(campaign["id"])
    c_entries = [e for e in entries if e["action"] == "campaign.created"]
    assert len(c_entries) == 1
    assert c_entries[0]["actor"] == "operator"
    assert c_entries[0]["entity_type"] == "campaign"


def test_campaign_activated_creates_audit_entry(client, campaign):
    c2 = add_campaign(client, "Campaign 2")
    client.patch(f"/campaigns/{c2['id']}/activate")
    entries = _get_audit(c2["id"])
    act_entries = [e for e in entries if e["action"] == "campaign.activated"]
    assert len(act_entries) == 1
    assert act_entries[0]["actor"] == "operator"


# ── Watcher events ─────────────────────────────────────────────────────────────

def test_watcher_created_creates_audit_entry(client, campaign):
    client.post("/watchers", json={"tool_id": "confluence_exporter",
                                   "token_type": "credential_object", "name": "W1"})
    entries = _get_audit(campaign["id"])
    w_entries = [e for e in entries if e["action"] == "watcher.created"]
    assert len(w_entries) == 1
    assert w_entries[0]["actor"] == "operator"


def test_watcher_deleted_creates_audit_entry(client, campaign):
    r = client.post("/watchers", json={"tool_id": "confluence_exporter",
                                       "token_type": "credential_object", "name": "W1"})
    watcher_id = r.json()["id"]
    client.delete(f"/watchers/{watcher_id}")
    entries = _get_audit(campaign["id"])
    del_entries = [e for e in entries if e["action"] == "watcher.deleted"]
    assert len(del_entries) == 1
    assert del_entries[0]["actor"] == "operator"


# ── Ingest key events ──────────────────────────────────────────────────────────

def test_ingest_key_created_creates_audit_entry(client, campaign):
    _gen_key(client, campaign["id"], name="new-key")
    entries = _get_audit(campaign["id"])
    created_entries = [e for e in entries if e["action"] == "ingest_key.created"]
    assert len(created_entries) == 1
    assert created_entries[0]["actor"] == "operator"
    assert created_entries[0]["detail"]["name"] == "new-key"


def test_ingest_key_created_detail_is_full_entity_snapshot(client, campaign):
    """detail must contain the key record fields — id/campaign_id are top-level, not in detail."""
    key_body = _gen_key(client, campaign["id"], name="snap-key")
    entries = _get_audit(campaign["id"])
    e = next(x for x in entries if x["action"] == "ingest_key.created")
    detail = e["detail"]
    assert detail["name"] == "snap-key"
    assert detail["key_hash"] is not None
    # id and campaign_id live on the audit row itself — must NOT be duplicated in detail
    assert "id" not in detail
    assert "campaign_id" not in detail
    # top-level fields carry the same values
    assert e["entity_id"] == key_body["id"]
    assert e["campaign_id"] == campaign["id"]


def test_ingest_key_revoke_creates_audit_entry(client, campaign):
    key_body = _gen_key(client, campaign["id"])
    client.delete(f"/ingest-keys/{key_body['id']}", headers=_auth())
    entries = _get_audit(campaign["id"])
    revoke_entries = [e for e in entries if e["action"] == "ingest_key.revoked"]
    assert len(revoke_entries) == 1
    assert revoke_entries[0]["actor"] == "operator"


def test_ingest_key_revoke_detail_includes_key_hash(client, campaign):
    """Revoke detail must also snapshot the full record including key_hash."""
    key_body = _gen_key(client, campaign["id"])
    client.delete(f"/ingest-keys/{key_body['id']}", headers=_auth())
    entries = _get_audit(campaign["id"])
    e = next(x for x in entries if x["action"] == "ingest_key.revoked")
    assert e["detail"]["key_hash"] is not None
    assert e["detail"]["name"] == "audit-key"


# ── Tool events ────────────────────────────────────────────────────────────────

def test_tool_upload_creates_audit_entry(client, campaign):
    import io
    from pathlib import Path
    import main as _main
    yaml_content = b"""id: test_tool\nname: Test Tool\ndescription: desc\ncommand: test\nparameters: []\n"""
    r = client.post("/tools", files={"file": ("test_tool.yaml", io.BytesIO(yaml_content), "text/yaml")},
                    headers=_auth())
    assert r.status_code == 200
    # Clean up: delete uploaded tool file so other tests see only real tools
    tool_path = Path(_main.TOOLS_DIR) / "test_tool.yaml"
    if tool_path.exists():
        tool_path.unlink()
    _main._reload_tools()
    entries = _get_audit(None)  # global event — campaign_id is None
    tool_entries = [e for e in entries if e["action"] == "tool.uploaded"]
    assert len(tool_entries) == 1
    assert tool_entries[0]["actor"] == "operator"


# ── WebSocket ──────────────────────────────────────────────────────────────────

def test_ws_init_includes_audit_log(client, campaign):
    add_token(client, "creds", CREDS, type="credential_object")
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert "audit_log" in msg
    assert any(e["action"] == "token.created" for e in msg["audit_log"])


def test_ws_broadcasts_audit_event(client, campaign, monkeypatch):
    import copy
    broadcasts = []

    async def fake_broadcast(m):
        broadcasts.append(copy.deepcopy(m))

    monkeypatch.setattr(main, "broadcast", fake_broadcast)
    add_token(client, "creds", CREDS, type="credential_object")
    audit_broadcasts = [b for b in broadcasts if b.get("type") == "audit_event"]
    assert len(audit_broadcasts) >= 1


# ── Endpoint ───────────────────────────────────────────────────────────────────

def test_audit_endpoint_returns_campaign_scoped_entries(client, campaign):
    add_token(client, "creds", CREDS, type="credential_object")
    entries = client.get("/audit").json()
    assert isinstance(entries, list)
    assert any(e["action"] == "token.created" for e in entries)


def test_audit_entries_ordered_newest_first(client, campaign):
    add_token(client, "a", CREDS, type="credential_object")
    add_token(client, "b", CREDS, type="credential_object")
    entries = client.get("/audit").json()
    token_entries = [e for e in entries if e["action"] == "token.created"]
    assert len(token_entries) >= 2
    # Newest first
    assert token_entries[0]["timestamp"] >= token_entries[1]["timestamp"]
