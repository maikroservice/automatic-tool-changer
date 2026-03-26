"""Tests for startup migration helpers."""
import pytest
from sqlalchemy import text

from database import engine, init_db, _add_column_if_missing, metadata_obj


@pytest.mark.asyncio
async def test_add_column_if_missing_adds_new_column():
    """Column that doesn't exist yet should be created."""
    async with engine.begin() as conn:
        # Remove the column by dropping and recreating the table without it
        await conn.execute(text("DROP TABLE IF EXISTS _migration_test"))
        await conn.execute(text("CREATE TABLE _migration_test (id TEXT PRIMARY KEY, name TEXT)"))

        await _add_column_if_missing(conn, "_migration_test", "extra_col", "VARCHAR")

        result = await conn.execute(text("PRAGMA table_info(_migration_test)"))
        cols = [row[1] for row in result]
        assert "extra_col" in cols

        await conn.execute(text("DROP TABLE _migration_test"))


@pytest.mark.asyncio
async def test_add_column_if_missing_is_idempotent():
    """Calling it when the column already exists must not raise."""
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS _migration_test"))
        await conn.execute(text("CREATE TABLE _migration_test (id TEXT PRIMARY KEY, existing_col TEXT)"))

        # Should not raise even though the column is already there
        await _add_column_if_missing(conn, "_migration_test", "existing_col", "VARCHAR")

        await conn.execute(text("DROP TABLE _migration_test"))


@pytest.mark.asyncio
async def test_init_db_is_idempotent():
    """Calling init_db twice must not raise."""
    await init_db()
    await init_db()


@pytest.mark.asyncio
async def test_webhook_auth_header_column_present_after_init():
    """campaigns table must have webhook_auth_header after init_db."""
    async with engine.begin() as conn:
        result = await conn.execute(text("PRAGMA table_info(campaigns)"))
        cols = [row[1] for row in result]
    assert "webhook_auth_header" in cols
