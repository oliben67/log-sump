"""POST /files/upload -- migration plan Phase 2. Confirms an uploaded file
is immediately queryable through the ordinary daemon-scoped endpoints by
the same key that uploaded it (the auto-provisioning local_upload.py does).
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import Settings
from log_sump.common.redis_keys import auth_key
from log_sump.server.app import create_app

API_KEY = "uploader-key"


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    # `permitted_daemons()` treats "key exists with an empty permitted set"
    # the same as "key is unknown" (RedisApiKeyAuthBackend: `if not members:
    # return None`) -- an upload-only key that has never been granted a real
    # daemon still needs *some* provisioned entry to be recognized as known
    # at all before its very first upload. A placeholder value is the
    # documented way to provision an upload-only key.
    await client.sadd(auth_key(API_KEY), "upload-only-placeholder")
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


async def test_upload_requires_api_key(client: AsyncClient) -> None:
    resp = await client.post("/files/upload", files={"file": ("app.log", b"hi", "text/plain")})
    assert resp.status_code == 401


async def test_upload_plain_log_file_and_query_it_back(client: AsyncClient) -> None:
    data = b"2026-08-14T12:00:00.000000000Z hello from upload\n"

    resp = await client.post(
        "/files/upload",
        files={"file": ("app.log", data, "text/plain")},
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["log_count"] == 1
    docker_host = body["docker_host"]

    records_resp = await client.get(
        "/records",
        params={"docker_host": docker_host, "start": "2026-08-14T00:00:00Z"},
        headers=_auth_headers(),
    )
    assert records_resp.status_code == 200
    records = records_resp.json()["records"]
    assert len(records) == 1
    assert records[0]["message"] == "hello from upload"


async def test_upload_publishes_an_update_sse_event(redis: FakeAsyncRedis) -> None:
    """Migration plan Phase 6: a connected /events client should learn new
    data landed without polling itself.
    """
    app = create_app(settings=Settings(), redis=redis)
    async with app.router.lifespan_context(app):
        queue = app.state.broadcaster.subscribe()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/files/upload",
                files={"file": ("app.log", b"2026-08-14T12:00:00Z hi\n", "text/plain")},
                headers=_auth_headers(),
            )
        assert resp.status_code == 200
        docker_host = resp.json()["docker_host"]
        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert event == {"type": "update", "docker_host": docker_host}
