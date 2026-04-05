import asyncio
import hashlib
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
import tool_loader
from database import engine, init_db

import secrets

import secrets as _secrets

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000/")
TOOLS_DIR = os.getenv("TOOLS_DIR", "tools")

# Token required by privileged endpoints (POST/DELETE /tools).
# Set APP_TOKEN in .env to pin a value; otherwise a fresh token is generated each start.
APP_TOKEN: str = os.getenv("APP_TOKEN") or _secrets.token_hex(64)

# Token issued to the UI via WebSocket init — required for privileged endpoints.
APP_TOKEN = secrets.token_hex(32)

# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Automatic Tool Changer", lifespan=lifespan)

# ── Auth dependency ────────────────────────────────────────────────────────────

def _require_app_token(x_atc_token: str = Header(...)):
    if not _secrets.compare_digest(x_atc_token, APP_TOKEN):
        raise HTTPException(403, "Invalid or missing app token")


# ── In-memory WebSocket list (not persisted) ───────────────────────────────────
active_ws: list[WebSocket] = []

# ── Tool registry (loaded from YAML tool definitions) ─────────────────────────
# TOOLS stores the full tool config.  Use _tool_api(t) to get the
# API-safe subset (id / name / description / parameters).
TOOLS: dict[str, dict] = {}


def _reload_tools() -> None:
    _, full = tool_loader.load_tools(TOOLS_DIR)
    TOOLS.clear()
    TOOLS.update(full)


def _tool_api(t: dict) -> dict:
    return {k: t[k] for k in ("id", "name", "description", "parameters") if k in t}


_reload_tools()


# ── Pydantic models ────────────────────────────────────────────────────────────

class AddTokenRequest(BaseModel):
    name: str
    value: Any
    type: Optional[str] = "text"
    campaign_id: Optional[str] = None
    metadata: Optional[dict] = None


class IngestRequest(BaseModel):
    source: str
    credentials: dict
    name: Optional[str] = None
    metadata: Optional[dict] = None


class CreateIngestKeyRequest(BaseModel):
    name: str


class CreateRunRequest(BaseModel):
    tool_id: str
    token_ids: list[str]
    parameters: Optional[dict] = {}
    campaign_id: Optional[str] = None


class CreateWatcherRequest(BaseModel):
    tool_id: str
    token_type: str
    parameters: Optional[dict] = {}
    name: Optional[str] = None
    campaign_id: Optional[str] = None


class CreateCampaignRequest(BaseModel):
    name: str
    webhook_url: Optional[str] = None
    webhook_secret: Optional[str] = None
    webhook_auth_header: Optional[str] = "Authorization"


class UpdateCampaignRequest(BaseModel):
    name: Optional[str] = None
    webhook_url: Optional[str] = None
    webhook_secret: Optional[str] = None
    webhook_auth_header: Optional[str] = None


class UpdateWatcherRequest(BaseModel):
    name: Optional[str] = None
    token_type: Optional[str] = None
    parameters: Optional[dict] = None


class UpdateTokenRequest(BaseModel):
    name: Optional[str] = None
    value: Optional[Any] = None
    type: Optional[str] = None
    metadata: Optional[dict] = None


class RunCallbackRequest(BaseModel):
    result: Any = None
    error: Optional[str] = None


# ── WebSocket broadcast ────────────────────────────────────────────────────────

async def broadcast(message: dict) -> None:
    dead = []
    for ws in active_ws:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        active_ws.remove(ws)


# ── Tool execution ─────────────────────────────────────────────────────────────

def execute_tool(tool_id: str, values: list[Any], params: dict) -> Any:
    tool = TOOLS.get(tool_id)
    if not tool:
        raise ValueError(f"Unknown tool: {tool_id}")
    return tool_loader.build_commands(tool, values, params)


async def run_in_background(run_id: str) -> None:
    await asyncio.sleep(0.3)

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        if not run:
            return

        run["status"] = "running"
        run["started_at"] = datetime.utcnow().isoformat()
        await db.db_update_run(conn, run_id, status="running", started_at=run["started_at"])

    await broadcast({"type": "run_updated", "run": run})
    await asyncio.sleep(0.8)

    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        token_ids = run["token_ids"] or []

        try:
            values = []
            for tid in token_ids:
                tok = await db.db_get_token(conn, tid)
                if tok:
                    values.append(tok["value"])

            result = execute_tool(run["tool_id"], values, run["parameters"] or {})
            completed_at = datetime.utcnow().isoformat()
            await db.db_update_run(conn, run_id,
                                   status="completed", result=result,
                                   completed_at=completed_at)
            run.update(status="completed", result=result, completed_at=completed_at)
        except Exception as exc:
            completed_at = datetime.utcnow().isoformat()
            await db.db_update_run(conn, run_id,
                                   status="failed", error=str(exc),
                                   completed_at=completed_at)
            run.update(status="failed", error=str(exc), completed_at=completed_at)

    await broadcast({"type": "run_updated", "run": run})

    # Auto-fire webhook for watcher-triggered runs
    if run.get("triggered_by") and run.get("status") == "completed":
        asyncio.create_task(_fire_webhook(run_id, PUBLIC_BASE_URL))


async def _fire_webhook(run_id: str, base_url: str) -> None:
    """POST the completed run's commands to the campaign webhook."""
    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        if not run or run["status"] not in ("completed", "triggered"):
            return
        campaign = await db.db_get_campaign(conn, run["campaign_id"])

    if not campaign or not campaign.get("webhook_url"):
        return

    callback_url = base_url.rstrip("/") + f"/runs/{run_id}/callback"
    payload = {
        "commands": run["result"] or [],
        "run_id": run_id,
        "campaign_id": run["campaign_id"],
        "callback_url": callback_url,
    }
    headers = {"Content-Type": "application/json"}
    if campaign.get("webhook_secret"):
        header_name = campaign.get("webhook_auth_header") or "Authorization"
        headers[header_name] = campaign["webhook_secret"]

    attempted_at = datetime.utcnow().isoformat()
    webhook_url = campaign["webhook_url"]
    log_id = str(uuid.uuid4())[:8]

    status_code: Optional[int] = None
    response_body: Optional[str] = None
    error_msg: Optional[str] = None
    success = False

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=payload, headers=headers)
        status_code = resp.status_code
        response_body = resp.text[:500]
        success = resp.status_code < 400

        if success:
            async with engine.begin() as conn:
                await db.db_update_run(conn, run_id, status="triggered",
                                       webhook_triggered_at=attempted_at)
            run.update(status="triggered", webhook_triggered_at=attempted_at)
        else:
            error_msg = f"HTTP {resp.status_code}: {response_body}"
            async with engine.begin() as conn:
                await db.db_update_run(conn, run_id,
                                       error=f"Webhook error: HTTP {resp.status_code}",
                                       webhook_triggered_at=attempted_at)
            run.update(error=f"Webhook error: HTTP {resp.status_code}",
                       webhook_triggered_at=attempted_at)

    except Exception as exc:
        error_msg = str(exc)
        async with engine.begin() as conn:
            await db.db_update_run(conn, run_id,
                                   error=f"Webhook error: {exc}",
                                   webhook_triggered_at=attempted_at)
        run.update(error=f"Webhook error: {exc}", webhook_triggered_at=attempted_at)

    # Persist log and broadcast
    log_entry = {
        "id": log_id, "run_id": run_id, "campaign_id": run["campaign_id"],
        "attempted_at": attempted_at, "url": webhook_url,
        "status_code": status_code, "response_body": response_body,
        "error": error_msg, "success": success,
    }
    async with engine.begin() as conn:
        await db.db_create_webhook_log(conn, **log_entry)
        run = await db.db_get_run(conn, run_id)

    await broadcast({"type": "run_updated", "run": run})
    await broadcast({"type": "webhook_log", "log": log_entry})


# ── Campaign endpoints ─────────────────────────────────────────────────────────

@app.post("/campaigns", summary="Create a campaign")
async def create_campaign(req: CreateCampaignRequest):
    campaign_id = str(uuid.uuid4())[:8]
    async with engine.begin() as conn:
        campaign = await db.db_create_campaign(
            conn, id=campaign_id, name=req.name,
            webhook_url=req.webhook_url, webhook_secret=req.webhook_secret,
            webhook_auth_header=req.webhook_auth_header,
            created_by=None,
        )
        c_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign_id, actor="operator",
            action="campaign.created", entity_type="campaign", entity_id=campaign_id,
            detail=dict(campaign))
        # If this is the first campaign, activate it automatically
        all_campaigns = await db.db_list_campaigns(conn)
        if len(all_campaigns) == 1:
            campaign = await db.db_activate_campaign(conn, campaign_id)

    await broadcast({"type": "campaign_added", "campaign": campaign})
    await broadcast({"type": "audit_event", "entry": c_audit})
    return campaign


@app.get("/campaigns", summary="List all campaigns")
async def list_campaigns():
    async with engine.begin() as conn:
        return await db.db_list_campaigns(conn)


@app.patch("/campaigns/{campaign_id}/activate", summary="Set the active campaign")
async def activate_campaign(campaign_id: str):
    async with engine.begin() as conn:
        campaign = await db.db_activate_campaign(conn, campaign_id)
        if not campaign:
            raise HTTPException(404, "Campaign not found")
        act_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign_id, actor="operator",
            action="campaign.activated", entity_type="campaign", entity_id=campaign_id,
            detail=dict(campaign))
    await broadcast({"type": "campaign_activated", "campaign": campaign})
    await broadcast({"type": "audit_event", "entry": act_audit})
    return campaign


@app.delete("/campaigns/{campaign_id}", summary="Delete a campaign")
async def delete_campaign(campaign_id: str):
    async with engine.begin() as conn:
        campaign = await db.db_get_campaign(conn, campaign_id)
        deleted = await db.db_delete_campaign(conn, campaign_id)
        del_audit = None
        if deleted and campaign:
            del_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=campaign_id, actor="operator",
                action="campaign.deleted", entity_type="campaign", entity_id=campaign_id,
                detail=_mask_campaign(campaign))
    if not deleted:
        raise HTTPException(404, "Campaign not found")
    await broadcast({"type": "campaign_deleted", "campaign_id": campaign_id})
    if del_audit:
        await broadcast({"type": "audit_event", "entry": del_audit})
    return {"status": "deleted"}


def _mask_campaign(c: dict) -> dict:
    """Return a campaign snapshot safe to store in the audit log (secret masked)."""
    masked = dict(c)
    if masked.get("webhook_secret"):
        masked["webhook_secret"] = "***"
    return masked


@app.patch("/campaigns/{campaign_id}", summary="Update a campaign")
async def update_campaign(campaign_id: str, req: UpdateCampaignRequest):
    async with engine.begin() as conn:
        before = await db.db_get_campaign(conn, campaign_id)
        if not before:
            raise HTTPException(404, "Campaign not found")
        campaign = await db.db_update_campaign(
            conn, campaign_id,
            **{k: v for k, v in req.model_dump().items() if v is not None},
        )
        upd_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                                 campaign_id=campaign_id, actor="operator",
                                 action="campaign.updated",
                                 entity_type="campaign", entity_id=campaign_id,
                                 detail={"before": _mask_campaign(before),
                                         "after": _mask_campaign(campaign)})
    await broadcast({"type": "campaign_updated", "campaign": campaign})
    await broadcast({"type": "audit_event", "entry": upd_audit})
    return campaign


# ── Token endpoints ────────────────────────────────────────────────────────────

@app.post("/add_token", summary="Add a new token")
async def add_token(req: AddTokenRequest):
    async with engine.begin() as conn:
        # Resolve campaign
        if req.campaign_id:
            campaign = await db.db_get_campaign(conn, req.campaign_id)
            if not campaign:
                raise HTTPException(400, f"Campaign not found: {req.campaign_id}")
        else:
            campaign = await db.db_get_active_campaign(conn)
            if not campaign:
                raise HTTPException(400, "No active campaign. Create and activate a campaign first.")

        campaign_id = campaign["id"]
        token_id = str(uuid.uuid4())[:8]
        token = await db.db_create_token(
            conn, id=token_id, campaign_id=campaign_id,
            name=req.name, value=req.value, type=req.type,
            created_by=None, meta=req.metadata or {},
        )
        token_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign_id, actor="operator", action="token.created",
            entity_type="token", entity_id=token_id,
            detail=dict(token))

        await broadcast({"type": "token_added", "token": token})
        await broadcast({"type": "audit_event", "entry": token_audit})

        # Auto-trigger matching active watchers for this campaign
        watchers = await db.db_list_watchers(conn, campaign_id)
        runs_to_start = []

        for watcher in watchers:
            if not watcher["active"]:
                continue
            if watcher["token_type"] != token["type"] and watcher["token_type"] != "*":
                continue
            if watcher["tool_id"] not in TOOLS:
                continue

            run_id = str(uuid.uuid4())[:8]
            run = await db.db_create_run(
                conn, id=run_id, campaign_id=campaign_id,
                tool_id=watcher["tool_id"],
                tool_name=TOOLS[watcher["tool_id"]]["name"],
                token_ids=[token_id], token_names=[token["name"]],
                parameters=watcher["parameters"] or {},
                triggered_by=watcher["id"], created_by=None,
            )
            run_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=campaign_id, actor=f"watcher:{watcher['id']}",
                action="run.created", entity_type="run", entity_id=run_id,
                detail=dict(run))
            await db.db_increment_watcher(conn, watcher["id"])
            updated_watcher = await db.db_get_watcher(conn, watcher["id"])

            await broadcast({"type": "run_created", "run": run})
            await broadcast({"type": "watcher_updated", "watcher": updated_watcher})
            await broadcast({"type": "audit_event", "entry": run_audit})
            runs_to_start.append(run_id)

    # Start background tasks outside the transaction
    for run_id in runs_to_start:
        asyncio.create_task(run_in_background(run_id))

    return {"id": token_id, "token": token}


# ── Ingest key management ──────────────────────────────────────────────────────

def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


@app.post("/campaigns/{campaign_id}/ingest-keys",
          summary="Generate an ingest key for a campaign",
          dependencies=[Depends(_require_app_token)])
async def create_ingest_key(campaign_id: str, req: CreateIngestKeyRequest):
    async with engine.begin() as conn:
        campaign = await db.db_get_campaign(conn, campaign_id)
        if not campaign:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")
        key = "atc_" + _secrets.token_hex(32)
        key_id = str(uuid.uuid4())[:8]
        record = await db.db_create_ingest_key(
            conn, id=key_id, campaign_id=campaign_id,
            name=req.name, key_hash=_hash_key(key),
        )
        key_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign_id, actor="operator",
            action="ingest_key.created", entity_type="ingest_key", entity_id=key_id,
            detail={**record, "key_hash": _hash_key(key)})
    await broadcast({"type": "ingest_key_added", "ingest_key": record})
    await broadcast({"type": "audit_event", "entry": key_audit})
    return {**record, "key": key}


@app.get("/campaigns/{campaign_id}/ingest-keys",
         summary="List ingest keys for a campaign",
         dependencies=[Depends(_require_app_token)])
async def list_ingest_keys(campaign_id: str):
    async with engine.begin() as conn:
        return await db.db_list_ingest_keys(conn, campaign_id)


@app.delete("/ingest-keys/{key_id}",
            summary="Revoke an ingest key",
            dependencies=[Depends(_require_app_token)])
async def delete_ingest_key(key_id: str):
    async with engine.begin() as conn:
        key_rec = await db.db_get_ingest_key_by_id(conn, key_id)
        deleted = await db.db_delete_ingest_key(conn, key_id)
        rev_audit = None
        if deleted and key_rec:
            rev_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=key_rec["campaign_id"], actor="operator",
                action="ingest_key.revoked", entity_type="ingest_key", entity_id=key_id,
                detail=dict(key_rec))
    if not deleted:
        raise HTTPException(404, f"Ingest key not found: {key_id}")
    await broadcast({"type": "ingest_key_deleted", "ingest_key_id": key_id})
    if rev_audit:
        await broadcast({"type": "audit_event", "entry": rev_audit})
    return {"deleted": key_id}


# ── Ingest endpoint ────────────────────────────────────────────────────────────

@app.post("/ingest", summary="Programmatic token ingestion for external collectors")
async def ingest(req: IngestRequest, x_ingest_key: str = Header(...)):
    if not req.credentials:
        raise HTTPException(422, "credentials must not be empty")

    async with engine.begin() as conn:
        key_record = await db.db_get_ingest_key_by_hash(conn, _hash_key(x_ingest_key))
        if not key_record:
            raise HTTPException(403, "Invalid or revoked ingest key")

        campaign_id = key_record["campaign_id"]
        await db.db_touch_ingest_key(conn, key_record["id"])

        token_id = str(uuid.uuid4())[:8]
        name = req.name or f"{req.source} @ {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}"
        meta = {**(req.metadata or {}), "source": req.source,
                "ingest_key_id": key_record["id"],
                "ingest_key_name": key_record["name"]}

        token = await db.db_create_token(
            conn, id=token_id, campaign_id=campaign_id,
            name=name, value=req.credentials, type="credential_object",
            created_by=None, meta=meta,
        )

        await broadcast({"type": "token_added", "token": token})

        event = await db.db_create_ingest_event(
            conn, id=str(uuid.uuid4())[:8],
            ingest_key_id=key_record["id"],
            ingest_key_name=key_record["name"],
            campaign_id=campaign_id,
            source=req.source,
            token_id=token_id,
        )
        token_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign_id,
            actor=f"ingest_key:{key_record['name']}",
            action="token.created", entity_type="token", entity_id=token_id,
            detail=dict(token))
        await broadcast({"type": "audit_event", "entry": token_audit})

        watchers = await db.db_list_watchers(conn, campaign_id)
        runs_to_start = []

        for watcher in watchers:
            if not watcher["active"]:
                continue
            if watcher["token_type"] != token["type"] and watcher["token_type"] != "*":
                continue
            if watcher["tool_id"] not in TOOLS:
                continue

            run_id = str(uuid.uuid4())[:8]
            run = await db.db_create_run(
                conn, id=run_id, campaign_id=campaign_id,
                tool_id=watcher["tool_id"],
                tool_name=TOOLS[watcher["tool_id"]]["name"],
                token_ids=[token_id], token_names=[token["name"]],
                parameters=watcher["parameters"] or {},
                triggered_by=watcher["id"], created_by=None,
            )
            run_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=campaign_id, actor=f"watcher:{watcher['id']}",
                action="run.created", entity_type="run", entity_id=run_id,
                detail=dict(run))
            await db.db_increment_watcher(conn, watcher["id"])
            updated_watcher = await db.db_get_watcher(conn, watcher["id"])

            await broadcast({"type": "run_created", "run": run})
            await broadcast({"type": "watcher_updated", "watcher": updated_watcher})
            await broadcast({"type": "audit_event", "entry": run_audit})
            runs_to_start.append(run_id)

    await broadcast({"type": "ingest_event", "event": event})

    for run_id in runs_to_start:
        asyncio.create_task(run_in_background(run_id))

    return {"id": token_id, "name": name, "token": token}


@app.get("/tokens", summary="List tokens for the active campaign")
async def list_tokens():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_tokens(conn, campaign["id"])


@app.get("/tokens/{token_id}/runs", summary="List runs triggered by a token")
async def list_runs_by_token(token_id: str):
    async with engine.begin() as conn:
        return await db.db_list_runs_by_token(conn, token_id)


@app.patch("/tokens/{token_id}", summary="Update a token")
async def update_token(token_id: str, req: UpdateTokenRequest):
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if "metadata" in updates:
        updates["meta"] = updates.pop("metadata")
    async with engine.begin() as conn:
        before = await db.db_get_token(conn, token_id)
        if not before:
            raise HTTPException(404, "Token not found")
        token = await db.db_update_token(conn, token_id, **updates)
        token_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=token["campaign_id"], actor="operator",
            action="token.updated", entity_type="token", entity_id=token_id,
            detail={"before": before, "after": token})
    await broadcast({"type": "token_updated", "token": token})
    await broadcast({"type": "audit_event", "entry": token_audit})
    return token


# ── Watcher endpoints ──────────────────────────────────────────────────────────

@app.post("/watchers", summary="Create a watcher")
async def create_watcher(req: CreateWatcherRequest):
    if req.tool_id not in TOOLS:
        raise HTTPException(400, f"Unknown tool: {req.tool_id}")

    async with engine.begin() as conn:
        if req.campaign_id:
            campaign = await db.db_get_campaign(conn, req.campaign_id)
        else:
            campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            raise HTTPException(400, "No active campaign.")

        watcher_id = str(uuid.uuid4())[:8]
        watcher = await db.db_create_watcher(
            conn, id=watcher_id, campaign_id=campaign["id"],
            tool_id=req.tool_id, tool_name=TOOLS[req.tool_id]["name"],
            token_type=req.token_type,
            parameters=req.parameters or {},
            name=req.name or f"{TOOLS[req.tool_id]['name']} on {req.token_type}",
            created_by=None,
        )
        w_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign["id"], actor="operator",
            action="watcher.created", entity_type="watcher", entity_id=watcher_id,
            detail=dict(watcher))

    await broadcast({"type": "watcher_added", "watcher": watcher})
    await broadcast({"type": "audit_event", "entry": w_audit})
    return watcher


@app.get("/watchers", summary="List watchers for the active campaign")
async def list_watchers():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_watchers(conn, campaign["id"])


@app.patch("/watchers/{watcher_id}/toggle", summary="Pause or resume a watcher")
async def toggle_watcher(watcher_id: str):
    async with engine.begin() as conn:
        watcher = await db.db_toggle_watcher(conn, watcher_id)
        toggle_audit = None
        if watcher:
            toggle_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=watcher["campaign_id"], actor="operator",
                action="watcher.toggled", entity_type="watcher", entity_id=watcher_id,
                detail={"active": watcher["active"]})
    if not watcher:
        raise HTTPException(404, "Watcher not found")
    await broadcast({"type": "watcher_updated", "watcher": watcher})
    if toggle_audit:
        await broadcast({"type": "audit_event", "entry": toggle_audit})
    return watcher


@app.delete("/watchers/{watcher_id}", summary="Delete a watcher")
async def delete_watcher(watcher_id: str):
    async with engine.begin() as conn:
        watcher = await db.db_get_watcher(conn, watcher_id)
        deleted = await db.db_delete_watcher(conn, watcher_id)
        del_audit = None
        if deleted and watcher:
            del_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
                campaign_id=watcher["campaign_id"], actor="operator",
                action="watcher.deleted", entity_type="watcher", entity_id=watcher_id,
                detail=dict(watcher))
    if not deleted:
        raise HTTPException(404, "Watcher not found")
    await broadcast({"type": "watcher_deleted", "watcher_id": watcher_id})
    if del_audit:
        await broadcast({"type": "audit_event", "entry": del_audit})
    return {"status": "deleted"}


@app.patch("/watchers/{watcher_id}", summary="Update a watcher")
async def update_watcher(watcher_id: str, req: UpdateWatcherRequest):
    async with engine.begin() as conn:
        before = await db.db_get_watcher(conn, watcher_id)
        if not before:
            raise HTTPException(404, "Watcher not found")
        watcher = await db.db_update_watcher(
            conn, watcher_id,
            **{k: v for k, v in req.model_dump().items() if v is not None},
        )
        watcher_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=watcher["campaign_id"], actor="operator",
            action="watcher.updated", entity_type="watcher", entity_id=watcher_id,
            detail={"before": before, "after": watcher})
    await broadcast({"type": "watcher_updated", "watcher": watcher})
    await broadcast({"type": "audit_event", "entry": watcher_audit})
    return watcher


# ── Tool endpoints ─────────────────────────────────────────────────────────────

@app.get("/tools", summary="List available tools")
async def list_tools():
    return [_tool_api(t) for t in TOOLS.values()]


@app.post("/tools", summary="Upload a tool YAML",
          dependencies=[Depends(_require_app_token)])
async def upload_tool(file: UploadFile):
    content = await file.read()
    try:
        config = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise HTTPException(400, f"Invalid YAML: {exc}")

    errors = tool_loader.validate_tool(config or {})
    if errors:
        raise HTTPException(400, {"errors": errors})

    tool_id = config["id"]
    path = Path(TOOLS_DIR) / f"{tool_id}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    _reload_tools()
    async with engine.begin() as conn:
        tool_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=None, actor="operator",
            action="tool.uploaded", entity_type="tool", entity_id=tool_id,
            detail=_tool_api(TOOLS[tool_id]))
    await broadcast({"type": "tools_updated", "tools": [_tool_api(t) for t in TOOLS.values()]})
    await broadcast({"type": "audit_event", "entry": tool_audit})
    return _tool_api(TOOLS[tool_id])


@app.delete("/tools/{tool_id}", summary="Remove a tool",
            dependencies=[Depends(_require_app_token)])
async def delete_tool(tool_id: str):
    if tool_id not in TOOLS:
        raise HTTPException(404, "Tool not found")
    tool_snapshot = _tool_api(TOOLS[tool_id])
    path = Path(TOOLS_DIR) / f"{tool_id}.yaml"
    if path.exists():
        path.unlink()
    _reload_tools()
    async with engine.begin() as conn:
        tool_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=None, actor="operator",
            action="tool.deleted", entity_type="tool", entity_id=tool_id,
            detail=tool_snapshot)
    await broadcast({"type": "tools_updated", "tools": [_tool_api(t) for t in TOOLS.values()]})
    await broadcast({"type": "audit_event", "entry": tool_audit})
    return {"status": "deleted"}


# ── Run endpoints ──────────────────────────────────────────────────────────────

@app.post("/runs", summary="Create and enqueue a tool run")
async def create_run(req: CreateRunRequest):
    if req.tool_id not in TOOLS:
        raise HTTPException(400, f"Unknown tool: {req.tool_id}")

    async with engine.begin() as conn:
        if req.campaign_id:
            campaign = await db.db_get_campaign(conn, req.campaign_id)
        else:
            campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            raise HTTPException(400, "No active campaign.")

        # Validate tokens exist
        token_names = []
        for tid in req.token_ids:
            tok = await db.db_get_token(conn, tid)
            if not tok:
                raise HTTPException(400, f"Token not found: {tid}")
            token_names.append(tok["name"])

        run_id = str(uuid.uuid4())[:8]
        run = await db.db_create_run(
            conn, id=run_id, campaign_id=campaign["id"],
            tool_id=req.tool_id, tool_name=TOOLS[req.tool_id]["name"],
            token_ids=req.token_ids, token_names=token_names,
            parameters=req.parameters or {},
            triggered_by=None, created_by=None,
        )
        run_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=campaign["id"], actor="operator",
            action="run.created", entity_type="run", entity_id=run_id,
            detail=dict(run))

    await broadcast({"type": "run_created", "run": run})
    await broadcast({"type": "audit_event", "entry": run_audit})
    asyncio.create_task(run_in_background(run_id))
    return run


@app.get("/webhook-logs", summary="List webhook call logs for the active campaign")
async def list_webhook_logs():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_webhook_logs(conn, campaign["id"])


@app.get("/runs", summary="List runs for the active campaign")
async def list_runs():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_runs(conn, campaign["id"])


@app.post("/runs/{run_id}/trigger", summary="Manually fire a run's webhook")
async def trigger_run(run_id: str, request: Request):
    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    if run["status"] != "completed":
        raise HTTPException(400, f"Run is not completed (status: {run['status']})")

    async with engine.begin() as conn:
        trigger_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=run["campaign_id"], actor="operator",
            action="run.webhook_triggered", entity_type="run", entity_id=run_id,
            detail={"run_id": run_id, "tool_id": run["tool_id"]})
    await broadcast({"type": "audit_event", "entry": trigger_audit})

    base_url = str(request.base_url)
    asyncio.create_task(_fire_webhook(run_id, base_url))
    return {"status": "firing"}


@app.post("/runs/{run_id}/callback", summary="Receive executor result callback")
async def run_callback(run_id: str, req: RunCallbackRequest):
    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        if not run:
            raise HTTPException(404, "Run not found")
        if req.error:
            await db.db_update_run(conn, run_id, status="failed_ext",
                                   webhook_result=req.result, error=req.error)
        else:
            await db.db_update_run(conn, run_id, status="success",
                                   webhook_result=req.result)
        run = await db.db_get_run(conn, run_id)
        cb_audit = await db.db_append_audit(conn, id=str(uuid.uuid4())[:8],
            campaign_id=run["campaign_id"], actor="executor",
            action="run.callback_received", entity_type="run", entity_id=run_id,
            detail={"status": run["status"], "error": req.error})
    await broadcast({"type": "run_updated", "run": run})
    await broadcast({"type": "audit_event", "entry": cb_audit})
    return {"status": "ok"}


# ── WebSocket ──────────────────────────────────────────────────────────────────

@app.get("/audit", summary="Immutable audit log for active campaign")
async def list_audit():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_audit(conn, campaign["id"])


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    active_ws.append(ws)

    async with engine.begin() as conn:
        campaigns = await db.db_list_campaigns(conn)
        active = await db.db_get_active_campaign(conn)
        active_id = active["id"] if active else None
        tokens = await db.db_list_tokens(conn, active_id) if active_id else []
        runs = await db.db_list_runs(conn, active_id) if active_id else []
        watchers = await db.db_list_watchers(conn, active_id) if active_id else []
        webhook_logs = await db.db_list_webhook_logs(conn, active_id) if active_id else []
        ingest_keys = await db.db_list_ingest_keys(conn, active_id) if active_id else []
        ingest_events = await db.db_list_ingest_events(conn, active_id) if active_id else []
        audit_log = await db.db_list_audit(conn, active_id) if active_id else []

    await ws.send_json({
        "type": "init",
        "campaigns": campaigns,
        "active_campaign_id": active_id,
        "tokens": tokens,
        "runs": runs,
        "tools": [_tool_api(t) for t in TOOLS.values()],
        "watchers": watchers,
        "webhook_logs": webhook_logs,
        "ingest_keys": ingest_keys,
        "ingest_events": ingest_events,
        "audit_log": audit_log,
        "app_token": APP_TOKEN,
    })

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in active_ws:
            active_ws.remove(ws)


# ── Static files (must be last) ────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
