"""
Shared fixtures.

Sets DATABASE_URL to a file-based SQLite test DB *before* importing main,
so the engine is pointed at the right place from the start.

Also creates static/ with a stub index.html so StaticFiles doesn't raise.
"""
import asyncio
import os
import sys
from pathlib import Path

# ── Point at test DB before any app import ─────────────────────────────────────
TEST_DB = Path(__file__).parent.parent / "test.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB}"

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Create static/ with a stub index.html so StaticFiles doesn't raise on import
_static = PROJECT_ROOT / "static"
_static.mkdir(exist_ok=True)
(_static / "index.html").touch()

import pytest                              # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main                                # noqa: E402
import database as db                      # noqa: E402
from database import engine, metadata_obj  # noqa: E402


# ── DB reset before every test ────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_state():
    """Drop and recreate all tables before every test (clean slate)."""
    async def _reset():
        async with engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_reset())
    loop.close()
    main.active_ws.clear()
    yield
    main.active_ws.clear()


@pytest.fixture()
def client() -> TestClient:
    with TestClient(main.app, raise_server_exceptions=True) as c:
        yield c


# ── Helpers shared across test modules ────────────────────────────────────────

def add_campaign(client: TestClient, name: str = "Test Campaign",
                 webhook_url: str = None, webhook_secret: str = None,
                 webhook_auth_header: str = "Authorization") -> dict:
    r = client.post("/campaigns", json={
        "name": name,
        "webhook_url": webhook_url,
        "webhook_secret": webhook_secret,
        "webhook_auth_header": webhook_auth_header,
    })
    assert r.status_code == 200
    return r.json()


def add_token(client: TestClient, name: str = "tok", value=None, **kwargs) -> dict:
    payload = {"name": name, "value": value if value is not None else name, **kwargs}
    r = client.post("/add_token", json=payload)
    assert r.status_code == 200
    return r.json()
