"""POST /session/start etc -- migration plan Phase 4. Router-level auth
scoping; the actual finish/download logic is covered in test_sessions.py.
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import Settings
from log_sump_common.redis_keys import auth_key
from log_sump_server.app import create_app

API_KEY = "session-key"


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


async def test_start_session_requires_daemon_access(client: AsyncClient) -> None:
    resp = await client.post(
        "/session/start", json={"docker_host": "daemon-b"}, headers=_auth_headers()
    )
    assert resp.status_code == 403


async def test_start_stop_and_download_session(client: AsyncClient) -> None:
    start_resp = await client.post(
        "/session/start", json={"docker_host": "daemon-a"}, headers=_auth_headers()
    )
    assert start_resp.status_code == 200
    session_id = start_resp.json()["session_id"]

    stop_resp = await client.post(f"/session/{session_id}/stop", headers=_auth_headers())
    assert stop_resp.status_code == 200

    status_resp = await client.get(f"/session/{session_id}/status", headers=_auth_headers())
    assert status_resp.json()["status"] == "completed"

    download_resp = await client.get(f"/session/{session_id}/download", headers=_auth_headers())
    assert download_resp.status_code == 200
    assert download_resp.content  # a real zip archive, non-empty header at least


async def test_stop_unknown_session_returns_404(client: AsyncClient) -> None:
    resp = await client.post("/session/rec999/stop", headers=_auth_headers())
    assert resp.status_code == 404


async def test_session_actions_require_api_key(client: AsyncClient) -> None:
    for method, path in [
        ("POST", "/session/rec1/stop"),
        ("GET", "/session/rec1/status"),
        ("GET", "/session/rec1/download"),
        ("POST", "/session/ttl"),
    ]:
        body = {"seconds": 10} if path.endswith("ttl") else None
        resp = await client.request(method, path, json=body)
        assert resp.status_code == 401, f"{method} {path} did not require auth"
