"""Integration tests for REST endpoints and WebSocket."""
import pytest
from fastapi.testclient import TestClient

from tests.conftest import add_campaign, add_token


@pytest.fixture(autouse=True)
def campaign(client):
    """Every API test needs an active campaign."""
    add_campaign(client, "Test Campaign")

CREDS = {"url": "https://company.atlassian.net/wiki", "email": "user@co.com", "api_token": "TOK", "auth_type": "basic"}
CONFLUENCE_PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md"}


# ── /add_token ─────────────────────────────────────────────────────────────────

def test_add_token_returns_id_and_token(client):
    r = client.post("/add_token", json={"name": "my-token", "value": "abc123"})
    assert r.status_code == 200
    body = r.json()
    assert "id" in body
    assert body["token"]["name"] == "my-token"
    assert body["token"]["value"] == "abc123"
    assert body["token"]["type"] == "text"
    assert "created_at" in body["token"]


def test_add_token_with_metadata(client):
    r = client.post("/add_token", json={
        "name": "tok", "value": "v",
        "metadata": {"source": "pipeline", "priority": "high"},
    })
    assert r.status_code == 200
    assert r.json()["token"]["meta"]["source"] == "pipeline"


def test_add_token_custom_type(client):
    r = client.post("/add_token", json={"name": "tok", "value": 42, "type": "number"})
    assert r.status_code == 200
    assert r.json()["token"]["type"] == "number"


def test_add_token_dict_value(client):
    r = client.post("/add_token", json={"name": "tok", "value": CREDS})
    assert r.status_code == 200
    assert r.json()["token"]["value"] == CREDS


def test_add_token_ids_are_unique(client):
    ids = {add_token(client, f"tok{i}")["id"] for i in range(5)}
    assert len(ids) == 5


# ── /tokens ────────────────────────────────────────────────────────────────────

def test_list_tokens_empty(client):
    assert client.get("/tokens").json() == []


def test_list_tokens_after_adding(client):
    add_token(client, "alpha", "v1")
    tokens = client.get("/tokens").json()
    assert len(tokens) == 1
    assert tokens[0]["name"] == "alpha"


def test_list_tokens_multiple(client):
    for name in ("a", "b", "c"):
        add_token(client, name)
    assert len(client.get("/tokens").json()) == 3


# ── /tools ─────────────────────────────────────────────────────────────────────

def test_list_tools_returns_all_tools(client):
    tool_ids = {t["id"] for t in client.get("/tools").json()}
    assert tool_ids == {"confluence_exporter", "jira_exporter"}


def test_list_tools_have_required_fields(client):
    for tool in client.get("/tools").json():
        assert "id" in tool
        assert "name" in tool
        assert "description" in tool
        assert "parameters" in tool
        assert isinstance(tool["parameters"], list)


def test_tool_parameters_have_required_fields(client):
    for tool in client.get("/tools").json():
        for param in tool["parameters"]:
            assert "name" in param
            assert "label" in param
            assert "type" in param


def test_confluence_exporter_has_expected_parameters(client):
    tool = next(t for t in client.get("/tools").json() if t["id"] == "confluence_exporter")
    param_names = {p["name"] for p in tool["parameters"]}
    assert {"scope", "scope_value", "format", "output_dir", "depth", "force",
            "mode", "list_only", "scraper_fallback", "debug"} == param_names


def test_confluence_exporter_scope_is_select(client):
    tool = next(t for t in client.get("/tools").json() if t["id"] == "confluence_exporter")
    scope_param = next(p for p in tool["parameters"] if p["name"] == "scope")
    assert scope_param["type"] == "select"
    assert set(scope_param["options"]) == {"space", "page", "recursive"}


# ── /runs (creation) ───────────────────────────────────────────────────────────

def test_create_run_returns_pending(client):
    token_id = add_token(client, "creds", CREDS)["id"]
    r = client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [token_id], "parameters": CONFLUENCE_PARAMS})
    assert r.status_code == 200
    run = r.json()
    assert run["status"] == "pending"
    assert run["tool_id"] == "confluence_exporter"
    assert run["tool_name"] == "Confluence Exporter"
    assert run["token_ids"] == [token_id]
    assert run["token_names"] == ["creds"]
    assert "id" in run
    assert "created_at" in run


def test_create_run_records_parameters(client):
    token_id = add_token(client, "creds", CREDS)["id"]
    r = client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [token_id], "parameters": CONFLUENCE_PARAMS})
    assert r.status_code == 200
    assert r.json()["parameters"] == CONFLUENCE_PARAMS


def test_create_run_multiple_tokens(client):
    ids = [add_token(client, f"creds{i}", CREDS)["id"] for i in range(3)]
    r = client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": ids, "parameters": CONFLUENCE_PARAMS})
    assert r.status_code == 200
    assert r.json()["token_ids"] == ids


def test_create_run_invalid_tool(client):
    token_id = add_token(client, "tok")["id"]
    r = client.post("/runs", json={"tool_id": "does_not_exist", "token_ids": [token_id]})
    assert r.status_code == 400


def test_create_run_invalid_token_id(client):
    r = client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": ["fake-id"]})
    assert r.status_code == 400


def test_create_run_mix_of_valid_and_invalid_tokens(client):
    valid_id = add_token(client, "tok")["id"]
    r = client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [valid_id, "bad-id"]})
    assert r.status_code == 400


# ── /runs (listing) ────────────────────────────────────────────────────────────

def test_list_runs_empty(client):
    assert client.get("/runs").json() == []


def test_list_runs_after_creating(client):
    token_id = add_token(client, "creds", CREDS)["id"]
    client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [token_id], "parameters": CONFLUENCE_PARAMS})
    assert len(client.get("/runs").json()) == 1


def test_list_runs_sorted_newest_first(client):
    token_id = add_token(client, "creds", CREDS)["id"]
    for _ in range(3):
        client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [token_id], "parameters": CONFLUENCE_PARAMS})
    runs = client.get("/runs").json()
    assert len(runs) == 3
    timestamps = [r["created_at"] for r in runs]
    assert timestamps == sorted(timestamps, reverse=True)


# ── WebSocket ──────────────────────────────────────────────────────────────────

def test_websocket_receives_init_message(client):
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert msg["type"] == "init"
    assert "tokens" in msg
    assert "runs" in msg
    assert "tools" in msg


def test_websocket_init_reflects_existing_tokens(client):
    add_token(client, "existing", "value")
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert len(msg["tokens"]) == 1
    assert msg["tokens"][0]["name"] == "existing"


def test_websocket_init_includes_confluence_exporter(client):
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    tool_ids = {t["id"] for t in msg["tools"]}
    assert "confluence_exporter" in tool_ids


def test_websocket_init_reflects_existing_runs(client):
    token_id = add_token(client, "creds", CREDS)["id"]
    client.post("/runs", json={"tool_id": "confluence_exporter", "token_ids": [token_id], "parameters": CONFLUENCE_PARAMS})
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert len(msg["runs"]) == 1
