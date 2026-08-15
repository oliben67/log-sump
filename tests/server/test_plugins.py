"""log_sump.server.plugins: the generic router-plugin loading mechanism.
Deliberately client-agnostic -- these tests only ever exercise generic
fixture plugins, never anything naming a specific client, matching what
the mechanism itself is for.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import PluginsConfig, Settings
from log_sump.server.app import create_app
from log_sump.server.plugins import load_plugin_routers

SIMPLE_PLUGIN_INIT = '''from fastapi import APIRouter

router = APIRouter()


@router.get("/plugin-ping")
async def ping():
    return {"ok": True}
'''

MULTI_FILE_PLUGIN_INIT = """from .routes import router
"""

MULTI_FILE_PLUGIN_ROUTES = '''from fastapi import APIRouter

router = APIRouter()


@router.get("/multi-file-ping")
async def ping():
    return {"ok": True}
'''

NO_ROUTER_PLUGIN_INIT = """value = 42
"""

BROKEN_PLUGIN_INIT = """raise RuntimeError("boom")
"""

NOT_A_ROUTER_PLUGIN_INIT = """router = "not actually an APIRouter"
"""

#: Collides with the built-in health router's own GET /health/live --
#: exercises the mount-time conflict check, not load_plugin_routers itself
#: (which has no idea what's already mounted).
CONFLICTING_PLUGIN_INIT = '''from fastapi import APIRouter

router = APIRouter()


@router.get("/health/live")
async def fake_health():
    return {"status": "not the real one"}
'''


#: A submodule holding module-level mutable state, the way a real plugin's
#: own in-process cache would (see legacy_gateway_api's log_index.py) --
#: `/bump` appends and returns the running count, so a test can tell
#: whether a second load actually re-executed this submodule fresh or
#: reused whatever `sys.modules` already had cached from the first load.
STATEFUL_PLUGIN_INIT = """from .state import router
"""

STATEFUL_PLUGIN_STATE = '''from fastapi import APIRouter

router = APIRouter()
calls: list[int] = []


@router.get("/bump")
async def bump():
    calls.append(1)
    return {"count": len(calls)}
'''


def _write_plugin(directory: Path, name: str, init_source: str) -> Path:
    plugin_dir = directory / name
    plugin_dir.mkdir()
    (plugin_dir / "__init__.py").write_text(init_source)
    return plugin_dir


def test_missing_directory_returns_empty(tmp_path: Path) -> None:
    assert load_plugin_routers(tmp_path / "does-not-exist") == []


def test_empty_directory_returns_empty(tmp_path: Path) -> None:
    assert load_plugin_routers(tmp_path) == []


def test_loads_a_simple_plugin(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "simple", SIMPLE_PLUGIN_INIT)

    loaded = load_plugin_routers(tmp_path)

    assert len(loaded) == 1
    name, router = loaded[0]
    assert name == "simple"
    assert isinstance(router, APIRouter)
    assert any(getattr(r, "path", None) == "/plugin-ping" for r in router.routes)


def test_loads_a_multi_file_plugin_with_relative_imports(tmp_path: Path) -> None:
    """The whole reason this is package-based (not exec'd like
    TransformRegistry's single-file convention): a plugin can be split
    across its own internal modules and import between them normally.
    """
    plugin_dir = _write_plugin(tmp_path, "multi", MULTI_FILE_PLUGIN_INIT)
    (plugin_dir / "routes.py").write_text(MULTI_FILE_PLUGIN_ROUTES)

    loaded = load_plugin_routers(tmp_path)

    assert len(loaded) == 1
    name, router = loaded[0]
    assert name == "multi"
    assert any(getattr(r, "path", None) == "/multi-file-ping" for r in router.routes)


def test_skips_a_directory_with_no_init_file(tmp_path: Path) -> None:
    (tmp_path / "not-a-plugin").mkdir()
    (tmp_path / "not-a-plugin" / "readme.txt").write_text("nothing here")

    assert load_plugin_routers(tmp_path) == []


def test_skips_a_plain_file_entry(tmp_path: Path) -> None:
    (tmp_path / "stray.py").write_text("x = 1")

    assert load_plugin_routers(tmp_path) == []


def test_skips_a_leading_underscore_directory(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "_disabled", SIMPLE_PLUGIN_INIT)

    assert load_plugin_routers(tmp_path) == []


def test_skips_a_plugin_with_no_router_attribute(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "no-router", NO_ROUTER_PLUGIN_INIT)

    assert load_plugin_routers(tmp_path) == []


def test_skips_a_plugin_whose_router_is_not_an_apirouter(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "wrong-type", NOT_A_ROUTER_PLUGIN_INIT)

    assert load_plugin_routers(tmp_path) == []


def test_a_broken_plugin_does_not_crash_the_whole_scan(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "broken", BROKEN_PLUGIN_INIT)
    _write_plugin(tmp_path, "healthy", SIMPLE_PLUGIN_INIT)

    loaded = load_plugin_routers(tmp_path)

    assert [name for name, _router in loaded] == ["healthy"]


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    yield FakeAsyncRedis()


async def test_create_app_mounts_a_configured_plugins_directory(
    tmp_path: Path, redis: FakeAsyncRedis
) -> None:
    _write_plugin(tmp_path, "simple", SIMPLE_PLUGIN_INIT)
    settings = Settings(plugins=PluginsConfig(directory=str(tmp_path)))
    app = create_app(settings=settings, redis=redis)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/plugin-ping")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


async def test_create_app_with_no_plugins_directory_configured_mounts_nothing_extra(
    redis: FakeAsyncRedis,
) -> None:
    app = create_app(settings=Settings(), redis=redis)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/plugin-ping")

    assert resp.status_code == 404


async def test_a_plugin_route_colliding_with_a_built_in_route_is_rejected(
    tmp_path: Path, redis: FakeAsyncRedis
) -> None:
    """A plugin must be additive-only: it can never make one of log-sump's
    own routes unreachable just by declaring a route at the same path --
    confirmed the hard way (see app.py's own comment) when a real plugin's
    legacy-compat routes shadowed log-sump's built-in `series` router.
    """
    _write_plugin(tmp_path, "conflicting", CONFLICTING_PLUGIN_INIT)
    settings = Settings(plugins=PluginsConfig(directory=str(tmp_path)))
    app = create_app(settings=settings, redis=redis)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/health/live")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}  # the real one, not the plugin's fake


async def test_a_non_conflicting_plugin_still_mounts_alongside_a_rejected_one(
    tmp_path: Path, redis: FakeAsyncRedis
) -> None:
    _write_plugin(tmp_path, "conflicting", CONFLICTING_PLUGIN_INIT)
    _write_plugin(tmp_path, "healthy", SIMPLE_PLUGIN_INIT)
    settings = Settings(plugins=PluginsConfig(directory=str(tmp_path)))
    app = create_app(settings=settings, redis=redis)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/plugin-ping")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


async def test_reloading_the_same_plugin_does_not_carry_over_submodule_state(
    tmp_path: Path,
) -> None:
    """A second `load_plugin_routers` call for the same plugin name (e.g.
    a process that calls `create_app()` more than once -- ordinary in a
    test suite, not just hypothetical) must genuinely re-execute the
    plugin fresh, not silently keep serving a submodule cached in
    `sys.modules` from the first load. Regression test for exactly the
    bug `_load_plugin_router`'s own comment describes.
    """
    plugin_dir = _write_plugin(tmp_path, "stateful", STATEFUL_PLUGIN_INIT)
    (plugin_dir / "state.py").write_text(STATEFUL_PLUGIN_STATE)

    _name1, router1 = load_plugin_routers(tmp_path)[0]
    _name2, router2 = load_plugin_routers(tmp_path)[0]

    assert router1 is not router2  # genuinely reloaded, not the same cached router
    app1 = FastAPI()
    app1.include_router(router1)
    app2 = FastAPI()
    app2.include_router(router2)
    async with (
        AsyncClient(transport=ASGITransport(app=app1), base_url="http://test") as ac1,
        AsyncClient(transport=ASGITransport(app=app2), base_url="http://test") as ac2,
    ):
        first_app_count = (await ac1.get("/bump")).json()["count"]
        second_app_count = (await ac2.get("/bump")).json()["count"]

    assert first_app_count == 1
    assert second_app_count == 1  # not 2 -- the second load's own `calls` list starts empty
