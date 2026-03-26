import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
from database import engine, init_db

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000/")

# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Automatic Tool Changer", lifespan=lifespan)

# ── In-memory WebSocket list (not persisted) ───────────────────────────────────
active_ws: list[WebSocket] = []

# ── Tool registry ──────────────────────────────────────────────────────────────
TOOLS: dict[str, dict] = {
    "confluence_exporter": {
        "id": "confluence_exporter",
        "name": "Confluence Exporter",
        "description": "Export Confluence pages or spaces. The token must be a credential_object with url, email, api_token, and optionally auth_type. Builds the confluence-exporter CLI command.",
        "parameters": [
            {
                "name": "scope",
                "type": "select",
                "label": "Scope",
                "options": ["space", "page", "recursive"],
                "default": "space",
                "required": True,
            },
            {
                "name": "scope_value",
                "type": "text",
                "label": "Space Key / Page URL or ID",
                "default": "",
                "placeholder": "MYSPACE  or  https://…/pages/123456",
                "required": True,
            },
            {
                "name": "format",
                "type": "select",
                "label": "Output Format",
                "options": ["md", "html", "raw"],
                "default": "md",
                "required": True,
            },
            {
                "name": "output_dir",
                "type": "text",
                "label": "Output Directory",
                "default": "./output",
                "placeholder": "./output",
                "required": False,
            },
            {
                "name": "depth",
                "type": "text",
                "label": "Max Depth",
                "default": "",
                "placeholder": "unlimited (recursive scope only)",
                "required": False,
            },
            {
                "name": "force",
                "type": "checkbox",
                "label": "Force overwrite existing files",
                "default": False,
                "required": False,
            },
        ],
    },
}


# ── Pydantic models ────────────────────────────────────────────────────────────

class AddTokenRequest(BaseModel):
    name: str
    value: Any
    type: Optional[str] = "text"
    campaign_id: Optional[str] = None
    metadata: Optional[dict] = None


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
    if tool_id == "confluence_exporter":
        scope     = params.get("scope", "space")
        scope_val = params.get("scope_value", "")
        fmt       = params.get("format", "md")
        out_dir   = params.get("output_dir", "./output")
        depth     = params.get("depth", "")
        force     = params.get("force") in (True, "true", "on", "yes", "1")

        commands = []
        for v in values:
            creds = v if isinstance(v, dict) else {}
            url        = creds.get("url", creds.get("confluence_url", ""))
            email      = creds.get("email", creds.get("confluence_email", ""))
            api_token  = creds.get("api_token", creds.get("confluence_token", ""))
            auth_type  = creds.get("auth_type", "basic")

            env = (
                f"CONFLUENCE_URL={url} "
                f"CONFLUENCE_AUTH_TYPE={auth_type} "
                f"CONFLUENCE_EMAIL={email} "
                f"CONFLUENCE_TOKEN={api_token}"
            )
            args = f"--{scope} {scope_val} --format {fmt} --output {out_dir}"
            if depth:
                args += f" --depth {depth}"
            if force:
                args += " --force"

            commands.append(f"{env} confluence-exporter {args}")
        return commands

    raise ValueError(f"Unknown tool: {tool_id}")


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
        # If this is the first campaign, activate it automatically
        all_campaigns = await db.db_list_campaigns(conn)
        if len(all_campaigns) == 1:
            campaign = await db.db_activate_campaign(conn, campaign_id)

    await broadcast({"type": "campaign_added", "campaign": campaign})
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
    await broadcast({"type": "campaign_activated", "campaign": campaign})
    return campaign


@app.delete("/campaigns/{campaign_id}", summary="Delete a campaign")
async def delete_campaign(campaign_id: str):
    async with engine.begin() as conn:
        deleted = await db.db_delete_campaign(conn, campaign_id)
    if not deleted:
        raise HTTPException(404, "Campaign not found")
    await broadcast({"type": "campaign_deleted", "campaign_id": campaign_id})
    return {"status": "deleted"}


@app.patch("/campaigns/{campaign_id}", summary="Update a campaign")
async def update_campaign(campaign_id: str, req: UpdateCampaignRequest):
    async with engine.begin() as conn:
        campaign = await db.db_update_campaign(
            conn, campaign_id,
            **{k: v for k, v in req.model_dump().items() if v is not None},
        )
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    await broadcast({"type": "campaign_updated", "campaign": campaign})
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

        await broadcast({"type": "token_added", "token": token})

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
            await db.db_increment_watcher(conn, watcher["id"])
            updated_watcher = await db.db_get_watcher(conn, watcher["id"])

            await broadcast({"type": "run_created", "run": run})
            await broadcast({"type": "watcher_updated", "watcher": updated_watcher})
            runs_to_start.append(run_id)

    # Start background tasks outside the transaction
    for run_id in runs_to_start:
        asyncio.create_task(run_in_background(run_id))

    return {"id": token_id, "token": token}


@app.get("/tokens", summary="List tokens for the active campaign")
async def list_tokens():
    async with engine.begin() as conn:
        campaign = await db.db_get_active_campaign(conn)
        if not campaign:
            return []
        return await db.db_list_tokens(conn, campaign["id"])


@app.patch("/tokens/{token_id}", summary="Update a token")
async def update_token(token_id: str, req: UpdateTokenRequest):
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if "metadata" in updates:
        updates["meta"] = updates.pop("metadata")
    async with engine.begin() as conn:
        token = await db.db_update_token(conn, token_id, **updates)
    if not token:
        raise HTTPException(404, "Token not found")
    await broadcast({"type": "token_updated", "token": token})
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

    await broadcast({"type": "watcher_added", "watcher": watcher})
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
    if not watcher:
        raise HTTPException(404, "Watcher not found")
    await broadcast({"type": "watcher_updated", "watcher": watcher})
    return watcher


@app.delete("/watchers/{watcher_id}", summary="Delete a watcher")
async def delete_watcher(watcher_id: str):
    async with engine.begin() as conn:
        deleted = await db.db_delete_watcher(conn, watcher_id)
    if not deleted:
        raise HTTPException(404, "Watcher not found")
    await broadcast({"type": "watcher_deleted", "watcher_id": watcher_id})
    return {"status": "deleted"}


@app.patch("/watchers/{watcher_id}", summary="Update a watcher")
async def update_watcher(watcher_id: str, req: UpdateWatcherRequest):
    async with engine.begin() as conn:
        watcher = await db.db_update_watcher(
            conn, watcher_id,
            **{k: v for k, v in req.model_dump().items() if v is not None},
        )
    if not watcher:
        raise HTTPException(404, "Watcher not found")
    await broadcast({"type": "watcher_updated", "watcher": watcher})
    return watcher


# ── Tool endpoints ─────────────────────────────────────────────────────────────

@app.get("/tools", summary="List available tools")
async def list_tools():
    return list(TOOLS.values())


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

    await broadcast({"type": "run_created", "run": run})
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

    base_url = str(request.base_url)
    asyncio.create_task(_fire_webhook(run_id, base_url))
    return {"status": "firing"}


@app.post("/runs/{run_id}/callback", summary="Receive Tracecat result callback")
async def run_callback(run_id: str, req: RunCallbackRequest):
    async with engine.begin() as conn:
        run = await db.db_get_run(conn, run_id)
        if not run:
            raise HTTPException(404, "Run not found")
        await db.db_update_run(conn, run_id,
                               webhook_result=req.result,
                               error=req.error or run.get("error"))
        run = await db.db_get_run(conn, run_id)
    await broadcast({"type": "run_updated", "run": run})
    return {"status": "ok"}


# ── WebSocket ──────────────────────────────────────────────────────────────────

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

    await ws.send_json({
        "type": "init",
        "campaigns": campaigns,
        "active_campaign_id": active_id,
        "tokens": tokens,
        "runs": runs,
        "tools": list(TOOLS.values()),
        "watchers": watchers,
        "webhook_logs": webhook_logs,
    })

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in active_ws:
            active_ws.remove(ws)


# ── Static files (must be last) ────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
