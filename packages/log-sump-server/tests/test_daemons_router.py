"""POST /daemons, DELETE /daemons/{id} -- migration plan Phase 3. Confirms
a registered daemon is immediately visible in /catalog to the key that
registered it (the auto-provisioning pattern also used by file upload).
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import DaemonConfig, Settings
from log_sump_common.daemon_registry import list_registered_daemons
from log_sump_common.redis_keys import auth_key
from log_sump_server.app import create_app

API_KEY = "daemon-manager-key"


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    # Same provisioning nuance as file upload (test_files_router.py): a key
    # needs *some* entry to be recognized as known before its first action.
    await client.sadd(auth_key(API_KEY), "placeholder")
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


async def test_create_daemon_requires_api_key(client: AsyncClient) -> None:
    resp = await client.post("/daemons", json={"id": "prod-a", "host": "10.0.0.5"})
    assert resp.status_code == 401


async def test_create_daemon_and_it_appears_in_catalog(
    client: AsyncClient, redis: FakeAsyncRedis
) -> None:
    resp = await client.post(
        "/daemons",
        json={"id": "prod-a", "host": "10.0.0.5", "user": "deploy", "transport": "ssh"},
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    assert resp.json() == {"id": "prod-a", "host": "10.0.0.5", "enabled": True}

    registered = await list_registered_daemons(redis)
    assert len(registered) == 1
    assert registered[0].host == "10.0.0.5"

    catalog_resp = await client.get("/catalog", headers=_auth_headers())
    assert catalog_resp.status_code == 200
    assert {e["id"] for e in catalog_resp.json()} == {"prod-a"}


async def test_delete_registered_daemon(client: AsyncClient) -> None:
    await client.post(
        "/daemons", json={"id": "prod-a", "host": "10.0.0.5"}, headers=_auth_headers()
    )

    resp = await client.delete("/daemons/prod-a", headers=_auth_headers())

    assert resp.status_code == 204
    catalog_resp = await client.get("/catalog", headers=_auth_headers())
    assert catalog_resp.json() == []


async def test_delete_unknown_daemon_returns_404(client: AsyncClient) -> None:
    resp = await client.delete("/daemons/nonexistent", headers=_auth_headers())
    assert resp.status_code == 404


async def test_delete_yaml_seeded_daemon_returns_404(redis: FakeAsyncRedis) -> None:
    """A YAML-configured daemon was never written to the registry hash --
    DELETE only ever removes what POST /daemons itself added.
    """
    settings = Settings(daemons=[DaemonConfig(id="yaml-daemon", host="10.0.0.9")])
    app = create_app(settings=settings, redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.delete("/daemons/yaml-daemon", headers=_auth_headers())
    assert resp.status_code == 404
