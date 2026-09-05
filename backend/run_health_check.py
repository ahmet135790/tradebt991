"""Run one production health cycle for a Render Cron Job."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import asyncpg
import httpx
from redis import asyncio as redis_async

from app.main import DATABASE_URL, REDIS_URL, run_health_checks
from app.v22_commercial import ensure_commercial_schema, load_secret, load_state


async def main() -> int:
    application = SimpleNamespace(state=SimpleNamespace())
    application.state.http = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=5), trust_env=False)
    application.state.db_pool = None
    application.state.redis_client = None
    application.state.health_lock = asyncio.Lock()
    application.state.health_snapshot = None
    application.state.v22_commercial = {"state": load_state(), "secret": load_secret()}
    try:
        try:
            application.state.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2, timeout=5)
            await ensure_commercial_schema(application)
        except Exception:
            application.state.db_pool = None
        if REDIS_URL:
            try:
                client = redis_async.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
                await client.ping()
                application.state.redis_client = client
            except Exception:
                application.state.redis_client = None
        snapshot = await run_health_checks(application)
        return 1 if snapshot.get("counts", {}).get("ERROR", 0) else 0
    except Exception:
        return 1
    finally:
        if application.state.redis_client is not None:
            await application.state.redis_client.aclose()
        if application.state.db_pool is not None:
            await application.state.db_pool.close()
        await application.state.http.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
