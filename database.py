"""
Database engine, table definitions, and CRUD helpers.

DATABASE_URL env var selects the backend:
  postgresql+asyncpg://atc:atc@localhost:5432/atc   (default)
  sqlite+aiosqlite:///./atc.db                       (fallback)
"""
import os
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    Boolean, Column, Index, Integer, MetaData, String, Table, text
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection, create_async_engine

# ── Engine setup ───────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./atc.db")
_is_postgres = DATABASE_URL.startswith("postgresql")

if _is_postgres:
    engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False)
else:
    from sqlalchemy.pool import NullPool
    engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)

# ── Column type helpers ────────────────────────────────────────────────────────
# JSONB on Postgres, JSON on SQLite (via TEXT + json serialisation by SQLAlchemy)

if _is_postgres:
    from sqlalchemy.dialects.postgresql import JSONB as _Json
else:
    from sqlalchemy import JSON as _Json  # type: ignore

# ── Table definitions ──────────────────────────────────────────────────────────

metadata_obj = MetaData()

campaigns_table = Table(
    "campaigns", metadata_obj,
    Column("id",             String,  primary_key=True),
    Column("name",           String,  nullable=False),
    Column("webhook_url",         String),
    Column("webhook_secret",      String),
    Column("webhook_auth_header", String),
    Column("is_active",      Boolean, default=False),
    Column("created_by",     String),
    Column("created_at",     String,  nullable=False),
    Column("meta",           _Json),
)

tokens_table = Table(
    "tokens", metadata_obj,
    Column("id",          String, primary_key=True),
    Column("campaign_id", String, nullable=False),
    Column("name",        String, nullable=False),
    Column("value",       _Json),
    Column("type",        String, default="text"),
    Column("created_by",  String),
    Column("created_at",  String, nullable=False),
    Column("meta",        _Json),
)

watchers_table = Table(
    "watchers", metadata_obj,
    Column("id",            String,  primary_key=True),
    Column("campaign_id",   String,  nullable=False),
    Column("tool_id",       String,  nullable=False),
    Column("tool_name",     String,  nullable=False),
    Column("token_type",    String,  nullable=False),
    Column("parameters",    _Json),
    Column("name",          String),
    Column("active",        Boolean, default=True),
    Column("trigger_count", Integer, default=0),
    Column("last_triggered",String),
    Column("created_by",    String),
    Column("created_at",    String,  nullable=False),
    Column("meta",          _Json),
)

tool_runs_table = Table(
    "tool_runs", metadata_obj,
    Column("id",                   String, primary_key=True),
    Column("campaign_id",          String, nullable=False),
    Column("tool_id",              String, nullable=False),
    Column("tool_name",            String, nullable=False),
    Column("token_ids",            _Json),
    Column("token_names",          _Json),
    Column("parameters",           _Json),
    Column("status",               String, default="pending"),
    Column("result",               _Json),
    Column("error",                String),
    Column("triggered_by",         String),
    Column("created_by",           String),
    Column("created_at",           String, nullable=False),
    Column("started_at",           String),
    Column("completed_at",         String),
    Column("webhook_triggered_at", String),
    Column("webhook_result",       _Json),
    Column("meta",                 _Json),
)

webhook_logs_table = Table(
    "webhook_logs", metadata_obj,
    Column("id",            String,  primary_key=True),
    Column("run_id",        String,  nullable=False),
    Column("campaign_id",   String,  nullable=False),
    Column("attempted_at",  String,  nullable=False),
    Column("url",           String),
    Column("status_code",   Integer),
    Column("response_body", String),
    Column("error",         String),
    Column("success",       Boolean, nullable=False),
)

ingest_keys_table = Table(
    "ingest_keys", metadata_obj,
    Column("id",          String, primary_key=True),
    Column("campaign_id", String, nullable=False),
    Column("name",        String, nullable=False),
    Column("key_hash",    String, nullable=False),
    Column("created_at",  String, nullable=False),
    Column("last_used_at", String),
)

ingest_events_table = Table(
    "ingest_events", metadata_obj,
    Column("id",              String, primary_key=True),
    Column("ingest_key_id",   String, nullable=False),
    Column("ingest_key_name", String, nullable=False),
    Column("campaign_id",     String, nullable=False),
    Column("source",          String),
    Column("token_id",        String),
    Column("ingested_at",     String, nullable=False),
)


# ── Schema init ────────────────────────────────────────────────────────────────

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(metadata_obj.create_all)
        # Add columns introduced after initial schema (safe to run on every startup)
        await _add_column_if_missing(conn, "campaigns", "webhook_auth_header", "VARCHAR")


async def _add_column_if_missing(conn, table: str, column: str, col_type: str) -> None:
    """Best-effort ADD COLUMN — silently skips if it already exists."""
    if _is_postgres:
        await conn.execute(text(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}"
        ))
    else:
        # SQLite: check pragma then alter
        result = await conn.execute(text(f"PRAGMA table_info({table})"))
        cols = [row[1] for row in result]
        if column not in cols:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))


# ── Generic helpers ────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()


def _row(row) -> dict:
    """Convert a SQLAlchemy Row to a plain dict."""
    return dict(row._mapping)


# ── Campaign CRUD ──────────────────────────────────────────────────────────────

async def db_create_campaign(conn: AsyncConnection, *, id: str, name: str,
                              webhook_url: Optional[str], webhook_secret: Optional[str],
                              webhook_auth_header: Optional[str],
                              created_by: Optional[str]) -> dict:
    row = {
        "id": id, "name": name,
        "webhook_url": webhook_url, "webhook_secret": webhook_secret,
        "webhook_auth_header": webhook_auth_header,
        "is_active": False, "created_by": created_by,
        "created_at": _now(), "meta": {},
    }
    await conn.execute(campaigns_table.insert().values(**row))
    return row


async def db_list_campaigns(conn: AsyncConnection) -> list[dict]:
    result = await conn.execute(
        campaigns_table.select().order_by(campaigns_table.c.created_at)
    )
    return [_row(r) for r in result]


async def db_get_campaign(conn: AsyncConnection, campaign_id: str) -> Optional[dict]:
    result = await conn.execute(
        campaigns_table.select().where(campaigns_table.c.id == campaign_id)
    )
    row = result.first()
    return _row(row) if row else None


async def db_get_active_campaign(conn: AsyncConnection) -> Optional[dict]:
    result = await conn.execute(
        campaigns_table.select().where(campaigns_table.c.is_active == True)  # noqa: E712
    )
    row = result.first()
    return _row(row) if row else None


async def db_activate_campaign(conn: AsyncConnection, campaign_id: str) -> Optional[dict]:
    # Deactivate all, then activate the target
    await conn.execute(
        campaigns_table.update().values(is_active=False)
    )
    await conn.execute(
        campaigns_table.update()
        .where(campaigns_table.c.id == campaign_id)
        .values(is_active=True)
    )
    return await db_get_campaign(conn, campaign_id)


async def db_delete_campaign(conn: AsyncConnection, campaign_id: str) -> bool:
    result = await conn.execute(
        campaigns_table.delete().where(campaigns_table.c.id == campaign_id)
    )
    return result.rowcount > 0


async def db_update_campaign(conn: AsyncConnection, campaign_id: str, **kwargs) -> Optional[dict]:
    allowed = {"name", "webhook_url", "webhook_secret", "webhook_auth_header"}
    values = {k: v for k, v in kwargs.items() if k in allowed}
    if not values:
        return await db_get_campaign(conn, campaign_id)
    result = await conn.execute(
        campaigns_table.update()
        .where(campaigns_table.c.id == campaign_id)
        .values(**values)
    )
    if result.rowcount == 0:
        return None
    return await db_get_campaign(conn, campaign_id)


# ── Token CRUD ─────────────────────────────────────────────────────────────────

async def db_create_token(conn: AsyncConnection, *, id: str, campaign_id: str,
                           name: str, value: Any, type: str,
                           created_by: Optional[str], meta: dict) -> dict:
    row = {
        "id": id, "campaign_id": campaign_id, "name": name,
        "value": value, "type": type,
        "created_by": created_by, "created_at": _now(), "meta": meta,
    }
    await conn.execute(tokens_table.insert().values(**row))
    return row


async def db_list_tokens(conn: AsyncConnection, campaign_id: str) -> list[dict]:
    result = await conn.execute(
        tokens_table.select()
        .where(tokens_table.c.campaign_id == campaign_id)
        .order_by(tokens_table.c.created_at)
    )
    return [_row(r) for r in result]


async def db_get_token(conn: AsyncConnection, token_id: str) -> Optional[dict]:
    result = await conn.execute(
        tokens_table.select().where(tokens_table.c.id == token_id)
    )
    row = result.first()
    return _row(row) if row else None


async def db_update_token(conn: AsyncConnection, token_id: str, **kwargs) -> Optional[dict]:
    allowed = {"name", "value", "type", "meta"}
    values = {k: v for k, v in kwargs.items() if k in allowed}
    if not values:
        return await db_get_token(conn, token_id)
    result = await conn.execute(
        tokens_table.update()
        .where(tokens_table.c.id == token_id)
        .values(**values)
    )
    if result.rowcount == 0:
        return None
    return await db_get_token(conn, token_id)


async def db_list_runs_by_token(conn: AsyncConnection, token_id: str) -> list[dict]:
    result = await conn.execute(
        tool_runs_table.select().order_by(tool_runs_table.c.created_at.desc())
    )
    return [_row(r) for r in result if token_id in (r.token_ids or [])]


# ── Watcher CRUD ───────────────────────────────────────────────────────────────

async def db_create_watcher(conn: AsyncConnection, *, id: str, campaign_id: str,
                             tool_id: str, tool_name: str, token_type: str,
                             parameters: dict, name: str,
                             created_by: Optional[str]) -> dict:
    row = {
        "id": id, "campaign_id": campaign_id,
        "tool_id": tool_id, "tool_name": tool_name,
        "token_type": token_type, "parameters": parameters, "name": name,
        "active": True, "trigger_count": 0, "last_triggered": None,
        "created_by": created_by, "created_at": _now(), "meta": {},
    }
    await conn.execute(watchers_table.insert().values(**row))
    return row


async def db_list_watchers(conn: AsyncConnection, campaign_id: str) -> list[dict]:
    result = await conn.execute(
        watchers_table.select()
        .where(watchers_table.c.campaign_id == campaign_id)
        .order_by(watchers_table.c.created_at)
    )
    return [_row(r) for r in result]


async def db_get_watcher(conn: AsyncConnection, watcher_id: str) -> Optional[dict]:
    result = await conn.execute(
        watchers_table.select().where(watchers_table.c.id == watcher_id)
    )
    row = result.first()
    return _row(row) if row else None


async def db_toggle_watcher(conn: AsyncConnection, watcher_id: str) -> Optional[dict]:
    watcher = await db_get_watcher(conn, watcher_id)
    if not watcher:
        return None
    new_active = not watcher["active"]
    await conn.execute(
        watchers_table.update()
        .where(watchers_table.c.id == watcher_id)
        .values(active=new_active)
    )
    watcher["active"] = new_active
    return watcher


async def db_increment_watcher(conn: AsyncConnection, watcher_id: str) -> None:
    await conn.execute(
        watchers_table.update()
        .where(watchers_table.c.id == watcher_id)
        .values(
            trigger_count=watchers_table.c.trigger_count + 1,
            last_triggered=_now(),
        )
    )


async def db_delete_watcher(conn: AsyncConnection, watcher_id: str) -> bool:
    result = await conn.execute(
        watchers_table.delete().where(watchers_table.c.id == watcher_id)
    )
    return result.rowcount > 0


async def db_update_watcher(conn: AsyncConnection, watcher_id: str, **kwargs) -> Optional[dict]:
    allowed = {"name", "token_type", "parameters"}
    values = {k: v for k, v in kwargs.items() if k in allowed}
    if not values:
        return await db_get_watcher(conn, watcher_id)
    result = await conn.execute(
        watchers_table.update()
        .where(watchers_table.c.id == watcher_id)
        .values(**values)
    )
    if result.rowcount == 0:
        return None
    return await db_get_watcher(conn, watcher_id)


# ── Tool run CRUD ──────────────────────────────────────────────────────────────

async def db_create_run(conn: AsyncConnection, *, id: str, campaign_id: str,
                         tool_id: str, tool_name: str, token_ids: list,
                         token_names: list, parameters: dict,
                         triggered_by: Optional[str],
                         created_by: Optional[str]) -> dict:
    row = {
        "id": id, "campaign_id": campaign_id,
        "tool_id": tool_id, "tool_name": tool_name,
        "token_ids": token_ids, "token_names": token_names,
        "parameters": parameters, "status": "pending",
        "result": None, "error": None,
        "triggered_by": triggered_by, "created_by": created_by,
        "created_at": _now(), "started_at": None, "completed_at": None,
        "webhook_triggered_at": None, "webhook_result": None, "meta": {},
    }
    await conn.execute(tool_runs_table.insert().values(**row))
    return row


async def db_list_runs(conn: AsyncConnection, campaign_id: str) -> list[dict]:
    result = await conn.execute(
        tool_runs_table.select()
        .where(tool_runs_table.c.campaign_id == campaign_id)
        .order_by(tool_runs_table.c.created_at.desc())
    )
    return [_row(r) for r in result]


async def db_get_run(conn: AsyncConnection, run_id: str) -> Optional[dict]:
    result = await conn.execute(
        tool_runs_table.select().where(tool_runs_table.c.id == run_id)
    )
    row = result.first()
    return _row(row) if row else None


async def db_update_run(conn: AsyncConnection, run_id: str, **kwargs) -> None:
    await conn.execute(
        tool_runs_table.update()
        .where(tool_runs_table.c.id == run_id)
        .values(**kwargs)
    )


# ── Webhook log CRUD ───────────────────────────────────────────────────────────

async def db_create_webhook_log(conn: AsyncConnection, *, id: str, run_id: str,
                                 campaign_id: str, attempted_at: str, url: str,
                                 status_code: Optional[int], response_body: Optional[str],
                                 error: Optional[str], success: bool) -> dict:
    row = {
        "id": id, "run_id": run_id, "campaign_id": campaign_id,
        "attempted_at": attempted_at, "url": url,
        "status_code": status_code, "response_body": response_body,
        "error": error, "success": success,
    }
    await conn.execute(webhook_logs_table.insert().values(**row))
    return row


async def db_list_webhook_logs(conn: AsyncConnection, campaign_id: str,
                                limit: int = 100) -> list[dict]:
    result = await conn.execute(
        webhook_logs_table.select()
        .where(webhook_logs_table.c.campaign_id == campaign_id)
        .order_by(webhook_logs_table.c.attempted_at.desc())
        .limit(limit)
    )
    return [_row(r) for r in result]


# ── Ingest key CRUD ────────────────────────────────────────────────────────────

async def db_create_ingest_key(conn: AsyncConnection, *, id: str, campaign_id: str,
                                name: str, key_hash: str) -> dict:
    row = {"id": id, "campaign_id": campaign_id, "name": name,
           "key_hash": key_hash, "created_at": _now(), "last_used_at": None}
    await conn.execute(ingest_keys_table.insert().values(**row))
    return {k: v for k, v in row.items() if k != "key_hash"}


async def db_list_ingest_keys(conn: AsyncConnection, campaign_id: str) -> list[dict]:
    result = await conn.execute(
        ingest_keys_table.select()
        .where(ingest_keys_table.c.campaign_id == campaign_id)
        .order_by(ingest_keys_table.c.created_at.desc())
    )
    return [{k: v for k, v in _row(r).items() if k != "key_hash"} for r in result]


async def db_get_ingest_key_by_hash(conn: AsyncConnection, key_hash: str) -> Optional[dict]:
    result = await conn.execute(
        ingest_keys_table.select().where(ingest_keys_table.c.key_hash == key_hash)
    )
    row = result.fetchone()
    return _row(row) if row else None


async def db_get_ingest_key_by_id(conn: AsyncConnection, key_id: str) -> Optional[dict]:
    result = await conn.execute(
        ingest_keys_table.select().where(ingest_keys_table.c.id == key_id)
    )
    row = result.fetchone()
    return _row(row) if row else None  # includes key_hash — safe for internal/audit use


async def db_delete_ingest_key(conn: AsyncConnection, key_id: str) -> bool:
    result = await conn.execute(
        ingest_keys_table.delete().where(ingest_keys_table.c.id == key_id)
    )
    return result.rowcount > 0


async def db_touch_ingest_key(conn: AsyncConnection, key_id: str) -> None:
    await conn.execute(
        ingest_keys_table.update()
        .where(ingest_keys_table.c.id == key_id)
        .values(last_used_at=_now())
    )


# ── Ingest event CRUD ──────────────────────────────────────────────────────────

RUN_STATUSES = ("pending", "running", "completed", "triggered", "success", "failed", "failed_ext")


async def db_create_ingest_event(conn: AsyncConnection, *, id: str, ingest_key_id: str,
                                  ingest_key_name: str, campaign_id: str,
                                  source: Optional[str], token_id: str) -> dict:
    row = {
        "id": id, "ingest_key_id": ingest_key_id, "ingest_key_name": ingest_key_name,
        "campaign_id": campaign_id, "source": source,
        "token_id": token_id, "ingested_at": _now(),
    }
    await conn.execute(ingest_events_table.insert().values(**row))
    return row


async def db_list_ingest_events(conn: AsyncConnection, campaign_id: str,
                                 limit: int = 200) -> list[dict]:
    result = await conn.execute(
        ingest_events_table.select()
        .where(ingest_events_table.c.campaign_id == campaign_id)
        .order_by(ingest_events_table.c.ingested_at.desc())
        .limit(limit)
    )
    return [_row(r) for r in result]


# ── Audit log ──────────────────────────────────────────────────────────────────

audit_log_table = Table(
    "audit_log", metadata_obj,
    Column("id",          String,  primary_key=True),
    Column("campaign_id", String),               # nullable for global events (tool upload)
    Column("actor",       String,  nullable=False),
    Column("action",      String,  nullable=False),
    Column("entity_type", String),
    Column("entity_id",   String),
    Column("detail",      _Json),
    Column("timestamp",   String,  nullable=False),
)
Index("ix_audit_log_campaign_id", audit_log_table.c.campaign_id)


async def db_append_audit(conn: AsyncConnection, *, id: str, campaign_id: Optional[str] = None,
                           actor: str, action: str, entity_type: Optional[str] = None,
                           entity_id: Optional[str] = None, detail: Optional[dict] = None) -> dict:
    # Strip fields already captured as top-level audit columns to avoid ambiguous duplicates
    if detail:
        detail = {k: v for k, v in detail.items() if k not in ("id", "campaign_id")}
    row = {
        "id": id, "campaign_id": campaign_id, "actor": actor,
        "action": action, "entity_type": entity_type,
        "entity_id": entity_id, "detail": detail, "timestamp": _now(),
    }
    await conn.execute(audit_log_table.insert().values(**row))
    return row


def _audit_select():
    """Audit rows joined with campaigns to include campaign_name."""
    j = audit_log_table.outerjoin(
        campaigns_table, audit_log_table.c.campaign_id == campaigns_table.c.id
    )
    return (
        audit_log_table.select()
        .add_columns(campaigns_table.c.name.label("campaign_name"))
        .select_from(j)
    )


async def db_list_audit(conn: AsyncConnection, campaign_id: Optional[str],
                         limit: int = 500) -> list[dict]:
    q = _audit_select()
    if campaign_id is not None:
        q = q.where(
            (audit_log_table.c.campaign_id == campaign_id) |
            (audit_log_table.c.campaign_id == None)  # noqa: E711
        )
    else:
        # global-only query (for tool events)
        q = q.where(audit_log_table.c.campaign_id == None)  # noqa: E711
    result = await conn.execute(
        q.order_by(audit_log_table.c.timestamp.desc()).limit(limit)
    )
    return [_row(r) for r in result]
