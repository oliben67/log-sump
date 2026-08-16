"""create_app()'s extra_routers seam: log-sump's extension point for
whatever a deployment needs beyond the built-in API surface. Deliberately
client-agnostic -- these tests only ever exercise generic fixture
routers, never anything naming a specific client, matching what the seam
itself is for.
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from fastapi import APIRouter
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import Settings
from log_sump.server.app import create_app

simple_router = APIRouter()


@simple_router.get("/extra-ping")
async def ping() -> dict[str, bool]:
    return {"ok": True}


#: Collides with the built-in health router's own GET /health/live --
#: exercises the mount-time conflict check.
conflicting_router = APIRouter()


@conflicting_router.get("/health/live")
async def fake_health() -> dict[str, str]:
    return {"status": "not the real one"}


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    yield FakeAsyncRedis()


async def test_create_app_mounts_extra_routers(redis: FakeAsyncRedis) -> None:
    app = create_app(settings=Settings(), redis=redis, extra_routers=[simple_router])

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/extra-ping")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


async def test_create_app_with_no_extra_routers_mounts_nothing_extra(
    redis: FakeAsyncRedis,
) -> None:
    app = create_app(settings=Settings(), redis=redis)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/extra-ping")

    assert resp.status_code == 404


async def test_an_extra_route_colliding_with_a_built_in_route_is_rejected(
    redis: FakeAsyncRedis,
) -> None:
    """An extra router must be additive-only: it can never make one of
    log-sump's own routes unreachable just by declaring a route at the
    same path -- confirmed the hard way (see app.py's own comment) back
    when this was still a runtime-loaded plugin, whose legacy-compat
    routes shadowed log-sump's built-in `series` router.
    """
    app = create_app(settings=Settings(), redis=redis, extra_routers=[conflicting_router])

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/health/live")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}  # the real one, not the extra router's


async def test_a_non_conflicting_router_still_mounts_alongside_a_rejected_one(
    redis: FakeAsyncRedis,
) -> None:
    app = create_app(
        settings=Settings(), redis=redis, extra_routers=[conflicting_router, simple_router]
    )

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/extra-ping")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


async def test_two_extra_routers_colliding_with_each_other_rejects_the_second(
    redis: FakeAsyncRedis,
) -> None:
    other_conflicting_router = APIRouter()

    @other_conflicting_router.get("/extra-ping")
    async def also_ping() -> dict[str, str]:
        return {"ok": "also"}

    app = create_app(
        settings=Settings(),
        redis=redis,
        extra_routers=[simple_router, other_conflicting_router],
    )

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/extra-ping")

    assert resp.json() == {"ok": True}  # simple_router's, mounted first
