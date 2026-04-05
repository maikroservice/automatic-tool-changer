"""Tests for run chain view — data contract (no UI assertions)."""
import pytest
from fastapi.testclient import TestClient

import main
from tests.conftest import add_campaign, add_token

APP_TOKEN = main.APP_TOKEN
CREDS = {"url": "https://co.atlassian.net", "email": "u@co.com", "api_token": "TOK"}
PARAMS = {"scope": "space", "scope_value": "DEV", "format": "md"}


@pytest.fixture(autouse=True)
def campaign(client):
    return add_campaign(client, "Test Campaign")


def _add_watcher(client, tool_id="confluence_exporter", token_type="credential_object"):
    r = client.post("/watchers", json={"tool_id": tool_id, "token_type": token_type, "name": "My Watcher"})
    assert r.status_code == 200
    return r.json()


def test_run_triggered_by_stores_watcher_id(client, campaign):
    """Watcher-triggered run stores the watcher id in triggered_by."""
    watcher = _add_watcher(client)
    tok = add_token(client, "creds", CREDS, type="credential_object")
    runs = client.get("/runs").json()
    auto_runs = [r for r in runs if r["triggered_by"] == watcher["id"]]
    assert len(auto_runs) == 1
    assert auto_runs[0]["triggered_by"] == watcher["id"]


def test_watcher_accessible_from_run_triggered_by(client, campaign):
    """The watcher referenced by run.triggered_by is retrievable by id."""
    watcher = _add_watcher(client)
    add_token(client, "creds", CREDS, type="credential_object")
    runs = client.get("/runs").json()
    auto_run = next(r for r in runs if r["triggered_by"] == watcher["id"])

    # Fetch all watchers and confirm the referenced watcher has a name
    watchers = {w["id"]: w for w in client.get("/watchers").json()}
    assert auto_run["triggered_by"] in watchers
    assert watchers[auto_run["triggered_by"]]["name"] == "My Watcher"
