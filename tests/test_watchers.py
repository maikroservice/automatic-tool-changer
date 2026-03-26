"""Integration tests for watchers and auto-trigger behaviour."""
import pytest
from tests.conftest import add_campaign, add_token

PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md"}
CREDS  = {"url": "https://co.atlassian.net/wiki", "email": "u@co.com", "api_token": "TOK", "auth_type": "basic"}


@pytest.fixture(autouse=True)
def campaign(client):
    """Every watcher test needs an active campaign."""
    add_campaign(client, "Test Campaign")


def add_watcher(client, token_type="credential_object", params=None, name=None):
    r = client.post("/watchers", json={
        "tool_id": "confluence_exporter",
        "token_type": token_type,
        "parameters": params or PARAMS,
        "name": name,
    })
    assert r.status_code == 200
    return r.json()


# ── CRUD ───────────────────────────────────────────────────────────────────────

def test_create_watcher_returns_watcher(client):
    w = add_watcher(client)
    assert "id" in w
    assert w["tool_id"] == "confluence_exporter"
    assert w["tool_name"] == "Confluence Exporter"
    assert w["token_type"] == "credential_object"
    assert w["active"] is True
    assert w["trigger_count"] == 0
    assert w["last_triggered"] is None


def test_create_watcher_auto_names_when_omitted(client):
    w = add_watcher(client)
    assert "Confluence Exporter" in w["name"]
    assert "credential_object" in w["name"]


def test_create_watcher_uses_provided_name(client):
    w = add_watcher(client, name="My Export Rule")
    assert w["name"] == "My Export Rule"


def test_create_watcher_invalid_tool(client):
    r = client.post("/watchers", json={"tool_id": "bad_tool", "token_type": "credentials"})
    assert r.status_code == 400


def test_list_watchers_empty(client):
    assert client.get("/watchers").json() == []


def test_list_watchers_after_creating(client):
    add_watcher(client)
    add_watcher(client, name="Second")
    assert len(client.get("/watchers").json()) == 2


def test_toggle_watcher_pauses_it(client):
    w = add_watcher(client)
    r = client.patch(f"/watchers/{w['id']}/toggle")
    assert r.status_code == 200
    assert r.json()["active"] is False


def test_toggle_watcher_resumes_it(client):
    w = add_watcher(client)
    client.patch(f"/watchers/{w['id']}/toggle")   # pause
    r = client.patch(f"/watchers/{w['id']}/toggle")  # resume
    assert r.json()["active"] is True


def test_toggle_watcher_not_found(client):
    assert client.patch("/watchers/nope/toggle").status_code == 404


def test_delete_watcher(client):
    w = add_watcher(client)
    r = client.delete(f"/watchers/{w['id']}")
    assert r.status_code == 200
    assert client.get("/watchers").json() == []


def test_delete_watcher_not_found(client):
    assert client.delete("/watchers/nope").status_code == 404


# ── Auto-trigger ───────────────────────────────────────────────────────────────

def test_matching_token_triggers_run(client):
    add_watcher(client, token_type="credential_object")
    add_token(client, "creds", CREDS, type="credential_object")
    runs = client.get("/runs").json()
    assert len(runs) == 1
    assert runs[0]["tool_id"] == "confluence_exporter"
    assert runs[0]["status"] in ("pending", "running", "completed")


def test_trigger_increments_trigger_count(client):
    w = add_watcher(client, token_type="credential_object")
    add_token(client, "creds", CREDS, type="credential_object")
    updated = next(x for x in client.get("/watchers").json() if x["id"] == w["id"])
    assert updated["trigger_count"] == 1


def test_trigger_sets_last_triggered(client):
    w = add_watcher(client, token_type="credential_object")
    add_token(client, "creds", CREDS, type="credential_object")
    updated = next(x for x in client.get("/watchers").json() if x["id"] == w["id"])
    assert updated["last_triggered"] is not None


def test_non_matching_type_does_not_trigger(client):
    add_watcher(client, token_type="credential_object")
    add_token(client, "tok", "hello", type="text")
    assert client.get("/runs").json() == []


def test_paused_watcher_does_not_trigger(client):
    w = add_watcher(client, token_type="credential_object")
    client.patch(f"/watchers/{w['id']}/toggle")  # pause
    add_token(client, "creds", CREDS, type="credential_object")
    assert client.get("/runs").json() == []


def test_multiple_watchers_all_trigger(client):
    add_watcher(client, name="Rule A")
    add_watcher(client, name="Rule B")
    add_token(client, "creds", CREDS, type="credential_object")
    assert len(client.get("/runs").json()) == 2


def test_wildcard_token_type_triggers_on_any(client):
    client.post("/watchers", json={
        "tool_id": "confluence_exporter",
        "token_type": "*",
        "parameters": PARAMS,
    })
    add_token(client, "tok", "anything", type="text")
    assert len(client.get("/runs").json()) == 1


def test_run_created_by_watcher_uses_watcher_params(client):
    custom = {"scope": "page", "scope_value": "99999", "format": "html"}
    add_watcher(client, params=custom)
    add_token(client, "creds", CREDS, type="credential_object")
    run = client.get("/runs").json()[0]
    assert run["parameters"] == custom


def test_run_created_by_watcher_has_triggered_by(client):
    w = add_watcher(client)
    add_token(client, "creds", CREDS, type="credential_object")
    run = client.get("/runs").json()[0]
    assert run["triggered_by"] == w["id"]


def test_token_not_yet_arrived_no_run(client):
    add_watcher(client, token_type="credential_object")
    # No token added — no runs
    assert client.get("/runs").json() == []


# ── WebSocket init includes watchers ──────────────────────────────────────────

def test_ws_init_includes_watchers(client):
    add_watcher(client, name="Test Rule")
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert "watchers" in msg
    assert len(msg["watchers"]) == 1
    assert msg["watchers"][0]["name"] == "Test Rule"
