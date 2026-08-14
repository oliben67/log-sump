"""POST /buffer/start etc -- migration plan Phase 4. Router-level auth
scoping; the actual snapshot/stop logic is covered in test_buffers.py.
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import Settings
from log_sump_common.redis_keys import auth_key
from log_sump_server.app import create_app

API_KEY = "buffer-key"


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    await client.sadd(auth_key(API_KEY), "daemon-a")
    yield client


@pytest.fixture
async def client(redis: FakeAsyncRedis) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=Settings(), redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


async def test_start_buffer_requires_daemon_access(client: AsyncClient) -> None:
    resp = await client.post(
        "/buffer/start", json={"docker_host": "daemon-b", "minutes": 5.0}, headers=_auth_headers()
    )
    assert resp.status_code == 403


async def test_start_pause_and_stop_buffer(client: AsyncClient) -> None:
    start_resp = await client.post(
        "/buffer/start", json={"docker_host": "daemon-a", "minutes": 5.0}, headers=_auth_headers()
    )
    assert start_resp.status_code == 200
    buffer_id = start_resp.json()["buffer_id"]

    pause_resp = await client.post(f"/buffer/{buffer_id}/pause", headers=_auth_headers())
    assert pause_resp.status_code == 200

    stop_resp = await client.post(f"/buffer/{buffer_id}/stop", headers=_auth_headers())
    assert stop_resp.status_code == 200
    assert stop_resp.content


async def test_pause_unknown_buffer_returns_404(client: AsyncClient) -> None:
    resp = await client.post("/buffer/b999/pause", headers=_auth_headers())
    assert resp.status_code == 404


async def test_buffer_actions_require_api_key(client: AsyncClient) -> None:
    resp = await client.post("/buffer/start", json={"docker_host": "daemon-a", "minutes": 5.0})
    assert resp.status_code == 401
