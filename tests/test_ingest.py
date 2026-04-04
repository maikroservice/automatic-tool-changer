"""Tests for ingest keys and POST /ingest."""
import pytest
from fastapi.testclient import TestClient

import main
from tests.conftest import add_campaign

APP_TOKEN = main.APP_TOKEN
CREDS = {"url": "https://company.atlassian.net", "email": "victim@company.com", "api_token": "TOK"}


@pytest.fixture(autouse=True)
def campaign(client):
    return add_campaign(client, "Test Campaign")


# ── WebSocket integration ──────────────────────────────────────────────────────

def test_ws_init_includes_ingest_keys(client, campaign):
    _gen_key(client, campaign["id"])
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert "ingest_keys" in msg
    assert len(msg["ingest_keys"]) == 1
    assert msg["ingest_keys"][0]["name"] == "test-key"
    assert "key" not in msg["ingest_keys"][0]
    assert "key_hash" not in msg["ingest_keys"][0]


def test_ws_init_ingest_keys_empty_by_default(client):
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert msg["ingest_keys"] == []


def test_ws_broadcasts_ingest_key_added(client, campaign):
    with client.websocket_connect("/ws") as ws:
        ws.receive_json()  # consume init
        client.post(f"/campaigns/{campaign['id']}/ingest-keys",
                    json={"name": "new-key"}, headers={"X-ATC-Token": APP_TOKEN})
        msg = ws.receive_json()
    assert msg["type"] == "ingest_key_added"
    assert msg["ingest_key"]["name"] == "new-key"
    assert "key" not in msg["ingest_key"]
    assert "key_hash" not in msg["ingest_key"]


def test_ws_broadcasts_ingest_key_deleted(client, campaign):
    body = _gen_key(client, campaign["id"])
    with client.websocket_connect("/ws") as ws:
        ws.receive_json()  # consume init
        client.delete(f"/ingest-keys/{body['id']}", headers={"X-ATC-Token": APP_TOKEN})
        msg = ws.receive_json()
    assert msg["type"] == "ingest_key_deleted"
    assert msg["ingest_key_id"] == body["id"]


def _auth(token=None):
    return {"X-ATC-Token": token or APP_TOKEN}


def _gen_key(client, campaign_id):
    """Generate an ingest key for a campaign, return the full response body."""
    r = client.post(f"/campaigns/{campaign_id}/ingest-keys",
                    json={"name": "test-key"}, headers=_auth())
    assert r.status_code == 200
    return r.json()


def _ingest(client, payload, key):
    headers = {"X-Ingest-Key": key} if key else {}
    return client.post("/ingest", json=payload, headers=headers)


# ── Ingest key management ──────────────────────────────────────────────────────

def test_generate_key_returns_plaintext_once(client, campaign):
    body = _gen_key(client, campaign["id"])
    assert "key" in body
    assert body["key"].startswith("atc_")


def test_generate_key_requires_app_token(client, campaign):
    r = client.post(f"/campaigns/{campaign['id']}/ingest-keys",
                    json={"name": "k"}, headers={"X-ATC-Token": "wrong"})
    assert r.status_code == 403


def test_generate_key_has_name_and_id(client, campaign):
    body = _gen_key(client, campaign["id"])
    assert body["name"] == "test-key"
    assert "id" in body


def test_list_keys_does_not_expose_plaintext(client, campaign):
    cid = campaign["id"]
    _gen_key(client, cid)
    keys = client.get(f"/campaigns/{cid}/ingest-keys", headers=_auth()).json()
    assert len(keys) == 1
    assert "key" not in keys[0]
    assert "key_hash" not in keys[0]
    assert keys[0]["name"] == "test-key"


def test_list_keys_empty(client, campaign):
    cid = campaign["id"]
    keys = client.get(f"/campaigns/{cid}/ingest-keys", headers=_auth()).json()
    assert keys == []


def test_revoke_key(client, campaign):
    cid = campaign["id"]
    body = _gen_key(client, cid)
    r = client.delete(f"/ingest-keys/{body['id']}", headers=_auth())
    assert r.status_code == 200
    assert client.get(f"/campaigns/{cid}/ingest-keys", headers=_auth()).json() == []


def test_revoked_key_cannot_ingest(client, campaign):
    cid = campaign["id"]
    body = _gen_key(client, cid)
    key = body["key"]
    client.delete(f"/ingest-keys/{body['id']}", headers=_auth())
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    assert r.status_code == 403


def test_multiple_keys_per_campaign(client, campaign):
    cid = campaign["id"]
    _gen_key(client, cid)
    _gen_key(client, cid)
    keys = client.get(f"/campaigns/{cid}/ingest-keys", headers=_auth()).json()
    assert len(keys) == 2


# ── POST /ingest auth ─────────────────────────────────────────────────────────

def test_ingest_requires_ingest_key(client):
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=None)
    assert r.status_code == 422


def test_ingest_rejects_wrong_key(client):
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key="atc_wrong")
    assert r.status_code == 403


def test_ingest_rejects_app_token(client, campaign):
    """APP_TOKEN must not be usable as an ingest key."""
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=APP_TOKEN)
    assert r.status_code == 403


# ── POST /ingest validation ────────────────────────────────────────────────────

def test_ingest_requires_source(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"credentials": CREDS}, key=key)
    assert r.status_code == 422


def test_ingest_requires_credentials(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish"}, key=key)
    assert r.status_code == 422


def test_ingest_credentials_must_be_dict(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": "not-a-dict"}, key=key)
    assert r.status_code == 422


def test_ingest_credentials_must_not_be_empty(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": {}}, key=key)
    assert r.status_code == 422


# ── POST /ingest success ──────────────────────────────────────────────────────

def test_ingest_creates_token(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    assert r.status_code == 200
    body = r.json()
    assert "id" in body
    assert "name" in body
    assert body["token"]["type"] == "credential_object"
    assert body["token"]["value"] == CREDS


def test_ingest_campaign_resolved_from_key(client, campaign):
    """No campaign_id in payload — resolved from the ingest key."""
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    assert r.status_code == 200
    tokens = client.get("/tokens").json()
    assert len(tokens) == 1


def test_ingest_auto_generates_name_from_source(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    assert "nophish" in r.json()["name"]


def test_ingest_accepts_custom_name(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": CREDS, "name": "victim-1"}, key=key)
    assert r.json()["name"] == "victim-1"


def test_ingest_source_stored_in_metadata(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    token_id = r.json()["id"]
    match = next(t for t in client.get("/tokens").json() if t["id"] == token_id)
    assert match["meta"]["source"] == "nophish"


def test_ingest_extra_metadata_stored(client, campaign):
    key = _gen_key(client, campaign["id"])["key"]
    r = _ingest(client, {
        "source": "nophish",
        "credentials": CREDS,
        "metadata": {"phis": "phis1", "victim_url": "http://hello.local/v1/"},
    }, key=key)
    token_id = r.json()["id"]
    match = next(t for t in client.get("/tokens").json() if t["id"] == token_id)
    assert match["meta"]["phis"] == "phis1"
    assert match["meta"]["source"] == "nophish"


def test_ingest_same_credentials_twice_creates_two_tokens(client, campaign):
    """Deduplication is the collector's responsibility."""
    key = _gen_key(client, campaign["id"])["key"]
    _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    _ingest(client, {"source": "nophish", "credentials": CREDS}, key=key)
    assert len(client.get("/tokens").json()) == 2
