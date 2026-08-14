"""GET /transforms -- migration plan Phase 5."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import Settings, TransformsConfig
from log_sump_common.redis_keys import auth_key
from log_sump_server.app import create_app

API_KEY = "transforms-key"

DROP_HEALTHCHECKS = '''"""Drops any log line mentioning a healthcheck."""

def transform(record):
    return None if "healthcheck" in record.get("message", "").lower() else record
'''


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    await client.sadd(auth_key(API_KEY), "daemon-a")
    yield client


async def _client(redis: FakeAsyncRedis, settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=settings, redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


async def test_transforms_empty_when_no_directory_configured(redis: FakeAsyncRedis) -> None:
    async for client in _client(redis, Settings()):
        resp = await client.get("/transforms", headers=_auth_headers())
        assert resp.status_code == 200
        assert resp.json() == {"transforms": []}


async def test_transforms_lists_configured_directory(redis: FakeAsyncRedis, tmp_path: Path) -> None:
    (tmp_path / "drop_healthchecks.py").write_text(DROP_HEALTHCHECKS)
    settings = Settings(transforms=TransformsConfig(directory=str(tmp_path)))

    async for client in _client(redis, settings):
        resp = await client.get("/transforms", headers=_auth_headers())
        assert resp.status_code == 200
        transforms = resp.json()["transforms"]
        assert transforms == [
            {"name": "drop_healthchecks", "doc": "Drops any log line mentioning a healthcheck."}
        ]


async def test_transforms_requires_api_key(redis: FakeAsyncRedis) -> None:
    async for client in _client(redis, Settings()):
        resp = await client.get("/transforms")
        assert resp.status_code == 401
