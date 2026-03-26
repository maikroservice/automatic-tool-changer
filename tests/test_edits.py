"""Integration tests for PATCH (edit) endpoints."""
import pytest
from tests.conftest import add_campaign, add_token

CREDS  = {"url": "https://co.atlassian.net/wiki", "email": "u@co.com", "api_token": "TOK", "auth_type": "basic"}
PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md"}


@pytest.fixture(autouse=True)
def campaign(client):
    add_campaign(client, "Test Campaign")


# ── Campaign edits ─────────────────────────────────────────────────────────────

def test_update_campaign_name(client):
    c = client.get("/campaigns").json()[0]
    r = client.patch(f"/campaigns/{c['id']}", json={"name": "Renamed"})
    assert r.status_code == 200
    assert r.json()["name"] == "Renamed"


def test_update_campaign_webhook(client):
    c = client.get("/campaigns").json()[0]
    r = client.patch(f"/campaigns/{c['id']}", json={
        "webhook_url": "https://tc.example.com/hooks/abc",
        "webhook_auth_header": "X-API-Key",
        "webhook_secret": "s3cr3t",
    })
    assert r.status_code == 200
    data = r.json()
    assert data["webhook_url"] == "https://tc.example.com/hooks/abc"
    assert data["webhook_auth_header"] == "X-API-Key"
    assert data["webhook_secret"] == "s3cr3t"


def test_update_campaign_not_found(client):
    assert client.patch("/campaigns/nope", json={"name": "X"}).status_code == 404


def test_update_campaign_preserves_other_fields(client):
    c = client.get("/campaigns").json()[0]
    client.patch(f"/campaigns/{c['id']}", json={"name": "New Name"})
    updated = client.get("/campaigns").json()[0]
    assert updated["is_active"] == c["is_active"]
    assert updated["created_at"] == c["created_at"]


# ── Token edits ────────────────────────────────────────────────────────────────

def test_update_token_name(client):
    tok = add_token(client, "original", "v")
    r = client.patch(f"/tokens/{tok['id']}", json={"name": "renamed"})
    assert r.status_code == 200
    assert r.json()["name"] == "renamed"


def test_update_token_value(client):
    tok = add_token(client, "tok", "old-value")
    r = client.patch(f"/tokens/{tok['id']}", json={"value": "new-value"})
    assert r.status_code == 200
    assert r.json()["value"] == "new-value"


def test_update_token_dict_value(client):
    tok = add_token(client, "tok", "old")
    r = client.patch(f"/tokens/{tok['id']}", json={"value": CREDS})
    assert r.status_code == 200
    assert r.json()["value"] == CREDS


def test_update_token_type(client):
    tok = add_token(client, "tok", "v", type="text")
    r = client.patch(f"/tokens/{tok['id']}", json={"type": "credential_object"})
    assert r.status_code == 200
    assert r.json()["type"] == "credential_object"


def test_update_token_not_found(client):
    assert client.patch("/tokens/nope", json={"name": "x"}).status_code == 404


def test_update_token_reflected_in_list(client):
    add_token(client, "tok", "v")
    tok_id = client.get("/tokens").json()[0]["id"]
    client.patch(f"/tokens/{tok_id}", json={"name": "updated"})
    tokens = client.get("/tokens").json()
    assert tokens[0]["name"] == "updated"


# ── Watcher edits ──────────────────────────────────────────────────────────────

def _add_watcher(client, name=None):
    r = client.post("/watchers", json={
        "tool_id": "confluence_exporter",
        "token_type": "credential_object",
        "parameters": PARAMS,
        "name": name or "My Rule",
    })
    assert r.status_code == 200
    return r.json()


def test_update_watcher_name(client):
    w = _add_watcher(client)
    r = client.patch(f"/watchers/{w['id']}", json={"name": "Renamed Rule"})
    assert r.status_code == 200
    assert r.json()["name"] == "Renamed Rule"


def test_update_watcher_token_type(client):
    w = _add_watcher(client)
    r = client.patch(f"/watchers/{w['id']}", json={"token_type": "*"})
    assert r.status_code == 200
    assert r.json()["token_type"] == "*"


def test_update_watcher_parameters(client):
    w = _add_watcher(client)
    new_params = {"scope": "page", "scope_value": "99999", "format": "html"}
    r = client.patch(f"/watchers/{w['id']}", json={"parameters": new_params})
    assert r.status_code == 200
    assert r.json()["parameters"] == new_params


def test_update_watcher_not_found(client):
    assert client.patch("/watchers/nope", json={"name": "x"}).status_code == 404


def test_update_watcher_preserves_active_state(client):
    w = _add_watcher(client)
    client.patch(f"/watchers/{w['id']}/toggle")  # pause
    client.patch(f"/watchers/{w['id']}", json={"name": "Still Paused"})
    updated = next(x for x in client.get("/watchers").json() if x["id"] == w["id"])
    assert updated["active"] is False
    assert updated["name"] == "Still Paused"
