"""Integration tests for campaign CRUD and activation."""
import pytest
from tests.conftest import add_campaign, add_token


# ── CRUD ───────────────────────────────────────────────────────────────────────

def test_create_campaign_returns_campaign(client):
    c = add_campaign(client, "Acme Corp")
    assert "id" in c
    assert c["name"] == "Acme Corp"
    assert c["is_active"] is True   # first campaign auto-activates
    assert c["webhook_url"] is None
    assert c["webhook_secret"] is None
    assert "created_at" in c


def test_create_campaign_with_webhook(client):
    c = add_campaign(client, "Acme", webhook_url="https://tc.example.com/webhook/abc",
                     webhook_secret="s3cr3t")
    assert c["webhook_url"] == "https://tc.example.com/webhook/abc"
    assert c["webhook_secret"] == "s3cr3t"


def test_first_campaign_auto_activates(client):
    c = add_campaign(client)
    assert c["is_active"] is True


def test_second_campaign_not_auto_activated(client):
    add_campaign(client, "First")
    c2 = add_campaign(client, "Second")
    assert c2["is_active"] is False


def test_list_campaigns_empty(client):
    assert client.get("/campaigns").json() == []


def test_list_campaigns_after_creating(client):
    add_campaign(client, "A")
    add_campaign(client, "B")
    assert len(client.get("/campaigns").json()) == 2


def test_delete_campaign(client):
    c = add_campaign(client)
    r = client.delete(f"/campaigns/{c['id']}")
    assert r.status_code == 200
    assert client.get("/campaigns").json() == []


def test_delete_campaign_not_found(client):
    assert client.delete("/campaigns/nope").status_code == 404


# ── Activation ─────────────────────────────────────────────────────────────────

def test_activate_campaign_sets_it_active(client):
    add_campaign(client, "First")
    c2 = add_campaign(client, "Second")
    r = client.patch(f"/campaigns/{c2['id']}/activate")
    assert r.status_code == 200
    assert r.json()["is_active"] is True


def test_activate_deactivates_previous(client):
    c1 = add_campaign(client, "First")
    c2 = add_campaign(client, "Second")
    client.patch(f"/campaigns/{c2['id']}/activate")
    campaigns = {c["id"]: c for c in client.get("/campaigns").json()}
    assert campaigns[c1["id"]]["is_active"] is False
    assert campaigns[c2["id"]]["is_active"] is True


def test_activate_not_found(client):
    assert client.patch("/campaigns/nope/activate").status_code == 404


def test_only_one_active_at_a_time(client):
    for name in ("A", "B", "C"):
        add_campaign(client, name)
    campaigns = client.get("/campaigns").json()
    active = [c for c in campaigns if c["is_active"]]
    assert len(active) == 1


# ── Campaign isolation ─────────────────────────────────────────────────────────

def test_tokens_scoped_to_active_campaign(client):
    c1 = add_campaign(client, "C1")
    add_token(client, "tok-c1", "v1")

    c2 = add_campaign(client, "C2")
    client.patch(f"/campaigns/{c2['id']}/activate")
    add_token(client, "tok-c2", "v2")

    # Switch back to C1 — should only see its token
    client.patch(f"/campaigns/{c1['id']}/activate")
    tokens = client.get("/tokens").json()
    assert len(tokens) == 1
    assert tokens[0]["name"] == "tok-c1"


def test_add_token_with_explicit_campaign_id(client):
    c1 = add_campaign(client, "C1")
    c2 = add_campaign(client, "C2")
    # Post token explicitly to c2 even though c1 is active
    r = client.post("/add_token", json={"name": "tok", "value": "v", "campaign_id": c2["id"]})
    assert r.status_code == 200

    # Active campaign is c1 — its token list should be empty
    assert client.get("/tokens").json() == []

    # Switch to c2 — token should appear
    client.patch(f"/campaigns/{c2['id']}/activate")
    assert len(client.get("/tokens").json()) == 1


def test_add_token_no_active_campaign_returns_400(client):
    # No campaign created — no active campaign
    r = client.post("/add_token", json={"name": "tok", "value": "v"})
    assert r.status_code == 400


def test_ws_init_includes_campaigns(client):
    add_campaign(client, "My Campaign")
    with client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()
    assert "campaigns" in msg
    assert "active_campaign_id" in msg
    assert len(msg["campaigns"]) == 1
    assert msg["campaigns"][0]["name"] == "My Campaign"
    assert msg["active_campaign_id"] == msg["campaigns"][0]["id"]
