"""Router-level coverage for /point, /index_at, /ticks, /series, /logs/find
(Phase 1 of the server migration) -- auth-scoping and response shape, not a
retest of queries.py's own algorithm coverage (see test_series_queries.py).
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import DaemonConfig, Settings
from log_sump.common.redis_keys import auth_key, stream_key
from log_sump.common.schema import Kind
from log_sump.server.app import create_app

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


async def test_point_forbidden_for_unpermitted_daemon(client: AsyncClient) -> None:
    resp = await client.get(
        "/point",
        params={"docker_host": "daemon-b", "t": "2026-08-14T12:00:00Z"},
        headers=_auth_headers(),
    )
    assert resp.status_code == 403


async def test_point_returns_nearest_metric_per_container(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    metric_json = (
        '{"kind":"metric","docker_host":"daemon-a","container_name":"web","container_id":"c1",'
        '"ts":"2026-08-14T12:00:00Z","seq":1,"metric_scope":"container","cpu_pct":42.0,'
        '"source":"docker stats"}'
    )
    await redis.xadd(
        stream_key("daemon-a", Kind.METRIC), {"data": metric_json}, id="1786729800000-0"
    )

    resp = await client.get(
        "/point",
        params={"docker_host": "daemon-a", "t": "2026-08-14T12:00:00Z"},
        headers=_auth_headers(),
    )

    assert resp.status_code == 200
    body = resp.json()
    # grouped by container_name ("web", no dot -- not a swarm task), not container_id
    assert body["services"]["web"]["cpu_pct"] == 42.0
    assert body["services"]["web"]["ttype"] == "container"


async def test_series_requires_daemon_access(client: AsyncClient) -> None:
    resp = await client.get(
        "/series",
        params={
            "docker_host": "daemon-b",
            "start": "2026-08-14T00:00:00Z",
            "end": "2026-08-14T23:59:59Z",
        },
        headers=_auth_headers(),
    )
    assert resp.status_code == 403


async def test_logs_find_returns_cursor_of_match(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    log_json = (
        '{"kind":"log","docker_host":"daemon-a","container_name":"web","container_id":"c1",'
        '"ts":"2026-08-14T12:00:00Z","seq":1,"stream":"stdout","level":"info",'
        '"message":"needle found here","fields":{},"raw":"needle found here"}'
    )
    await redis.xadd(stream_key("daemon-a", Kind.LOG), {"data": log_json}, id="1786729800000-0")

    resp = await client.get(
        "/logs/find",
        params={"docker_host": "daemon-a", "container_id": "c1", "q": "needle"},
        headers=_auth_headers(),
    )

    assert resp.status_code == 200
    assert resp.json()["cursor"] == "1786729800000-0"


async def test_services_returns_latest_listing_cycle(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    service_json = (
        '{"kind":"service","docker_host":"daemon-a","ts":"2026-08-14T12:00:00Z","seq":1,'
        '"id":"s1","name":"web","replicas":"3/3"}'
    )
    await redis.xadd(
        stream_key("daemon-a", Kind.SERVICE), {"data": service_json}, id="1786729800000-0"
    )

    resp = await client.get(
        "/services", params={"docker_host": "daemon-a"}, headers=_auth_headers()
    )

    assert resp.status_code == 200
    assert resp.json()["services"] == [{"id": "s1", "name": "web", "replicas": "3/3"}]


async def test_services_empty_on_a_non_swarm_daemon(client: AsyncClient) -> None:
    resp = await client.get(
        "/services", params={"docker_host": "daemon-a"}, headers=_auth_headers()
    )
    assert resp.status_code == 200
    assert resp.json()["services"] == []


async def test_all_series_endpoints_require_api_key(client: AsyncClient) -> None:
    common = {"docker_host": "daemon-a"}
    window = {"start": "2026-08-14T00:00:00Z", "end": "2026-08-14T23:59:59Z"}
    endpoints = [
        ("/point", {**common, "t": "2026-08-14T12:00:00Z"}),
        ("/index_at", {**common, "container_id": "c1", "t": "2026-08-14T12:00:00Z"}),
        ("/ticks", {**common, "container_id": "c1", **window}),
        ("/series", {**common, **window}),
        ("/logs/find", {**common, "container_id": "c1", "q": "x"}),
        ("/services", common),
    ]
    for path, params in endpoints:
        resp = await client.get(path, params=params)
        assert resp.status_code == 401, f"{path} did not require auth"
