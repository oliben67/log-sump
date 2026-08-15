"""POST /scheduler/create etc -- migration plan Phase 4. Router-level auth
scoping; the actual fire/cron logic is covered in test_scheduling.py.
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import Settings
from log_sump.common.redis_keys import auth_key
from log_sump.server.app import create_app

API_KEY = "scheduler-key"


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


async def test_create_schedule_requires_daemon_access(client: AsyncClient) -> None:
    resp = await client.post(
        "/scheduler/create",
        json={"docker_host": "daemon-b", "duration_minutes": 5.0, "cron": "* * * * *"},
        headers=_auth_headers(),
    )
    assert resp.status_code == 403


async def test_create_rejects_neither_start_at_nor_cron(client: AsyncClient) -> None:
    resp = await client.post(
        "/scheduler/create",
        json={"docker_host": "daemon-a", "duration_minutes": 5.0},
        headers=_auth_headers(),
    )
    assert resp.status_code == 400


async def test_create_status_and_cancel_schedule(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/scheduler/create",
        json={"docker_host": "daemon-a", "duration_minutes": 5.0, "cron": "* * * * *"},
        headers=_auth_headers(),
    )
    assert create_resp.status_code == 200
    schedule_id = create_resp.json()["schedule_id"]

    status_resp = await client.get(f"/scheduler/{schedule_id}", headers=_auth_headers())
    assert status_resp.status_code == 200
    assert status_resp.json()["cron"] == "* * * * *"

    cancel_resp = await client.post(f"/scheduler/{schedule_id}/cancel", headers=_auth_headers())
    assert cancel_resp.status_code == 200

    gone_resp = await client.get(f"/scheduler/{schedule_id}", headers=_auth_headers())
    assert gone_resp.status_code == 404


async def test_scheduler_actions_require_api_key(client: AsyncClient) -> None:
    resp = await client.post(
        "/scheduler/create",
        json={"docker_host": "daemon-a", "duration_minutes": 5.0, "cron": "* * * * *"},
    )
    assert resp.status_code == 401
