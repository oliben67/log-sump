from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import DaemonConfig, Settings
from log_sump_common.redis_keys import auth_key, stream_key
from log_sump_common.schema import Kind
from log_sump_server.app import create_app

API_KEY = "test-key"


def _settings() -> Settings:
    return Settings(
        daemons=[
            DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local"),
            DaemonConfig(id="daemon-b", host="10.0.0.2", transport="local"),
        ]
    )


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    await client.sadd(auth_key(API_KEY), "daemon-a")
    yield client


@pytest.fixture
async def client(redis: FakeAsyncRedis) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=_settings(), redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


async def test_health_live_requires_no_auth(client: AsyncClient) -> None:
    resp = await client.get("/health/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_health_ready_pings_redis(client: AsyncClient) -> None:
    resp = await client.get("/health/ready")
    assert resp.status_code == 200


async def test_catalog_requires_api_key(client: AsyncClient) -> None:
    resp = await client.get("/catalog")
    assert resp.status_code == 401


async def test_catalog_filters_to_permitted_daemons(client: AsyncClient) -> None:
    resp = await client.get("/catalog", headers=_auth_headers())
    assert resp.status_code == 200
    ids = [entry["id"] for entry in resp.json()]
    assert ids == ["daemon-a"]  # daemon-b exists in config but key isn't permitted for it


async def test_catalog_rejects_unknown_key(client: AsyncClient) -> None:
    resp = await client.get("/catalog", headers={"X-API-Key": "not-a-real-key"})
    assert resp.status_code == 401


async def test_records_forbidden_for_unpermitted_daemon(client: AsyncClient) -> None:
    resp = await client.get("/records", params={"docker_host": "daemon-b"}, headers=_auth_headers())
    assert resp.status_code == 403


async def test_records_returns_interleaved_logs_and_metrics(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    log_json = (
        '{"kind":"log","docker_host":"daemon-a","container_name":"web","container_id":"c1",'
        '"ts":"2026-08-14T12:00:01Z","seq":1,"stream":"stdout","level":"info",'
        '"message":"hello","fields":{},"raw":"hello"}'
    )
    metric_json = (
        '{"kind":"metric","docker_host":"daemon-a","container_name":"web","container_id":"c1",'
        '"ts":"2026-08-14T12:00:00Z","seq":1,"metric_scope":"container","source":"docker stats"}'
    )
    await redis.xadd(stream_key("daemon-a", Kind.LOG), {"data": log_json})
    await redis.xadd(stream_key("daemon-a", Kind.METRIC), {"data": metric_json})

    resp = await client.get(
        "/records",
        params={"docker_host": "daemon-a", "start": "2026-08-14T00:00:00Z"},
        headers=_auth_headers(),
    )

    assert resp.status_code == 200
    body = resp.json()
    kinds = [r["kind"] for r in body["records"]]
    assert kinds == ["metric", "log"]  # metric ts=12:00:00 sorts before log ts=12:00:01


async def test_admin_redis_command_requires_api_key(client: AsyncClient) -> None:
    resp = await client.post("/admin/redis/command", json={"command": "PING", "args": []})
    assert resp.status_code == 401


async def test_admin_redis_command_allows_ping(client: AsyncClient) -> None:
    resp = await client.post(
        "/admin/redis/command", json={"command": "PING", "args": []}, headers=_auth_headers()
    )
    assert resp.status_code == 200
    assert resp.json()["result"] is True


async def test_admin_redis_command_rejects_disallowed_command(client: AsyncClient) -> None:
    resp = await client.post(
        "/admin/redis/command",
        json={"command": "FLUSHALL", "args": []},
        headers=_auth_headers(),
    )
    assert resp.status_code == 403


async def test_admin_redis_command_any_permitted_key_can_inspect_any_daemon_data(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    # API_KEY is only permitted for daemon-a's /records, but the inspection
    # endpoint isn't daemon-scoped -- confirmed design (any valid key).
    await redis.set("some-key", "some-value")
    resp = await client.post(
        "/admin/redis/command",
        json={"command": "GET", "args": ["some-key"]},
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    assert resp.json()["result"] == "some-value"
