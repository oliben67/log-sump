"""GET /transforms -- migration plan Phase 5."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from fastapi import HTTPException, Request
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import GatewayConfig, Settings, TransformsConfig
from log_sump.common.redis_keys import auth_key
from log_sump.server.app import create_app
from log_sump.server.deps import require_valid_api_key_or_gateway_token

API_KEY = "transforms-key"
GATEWAY_TOKEN = "gw-secret"

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


async def test_transforms_accepts_gateway_token_alone(redis: FakeAsyncRedis) -> None:
    """br-PLUG-002 / BUG-0098: cttc's renderer (set-dialog.ts:413) never
    holds a daemon-scoped API key, only the shared gateway token -- this
    route 401'd for every such caller until require_valid_api_key_or_
    gateway_token replaced the api-key-only dependency.
    """
    settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
    async for client in _client(redis, settings):
        resp = await client.get("/transforms", headers={"X-CTTC-Token": GATEWAY_TOKEN})
        assert resp.status_code == 200
        assert resp.json() == {"transforms": []}


def _request(*, headers: dict[str, str] | None = None) -> Request:
    scope = {
        "type": "http",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "query_string": b"",
    }
    return Request(scope)


class TestCombinedAuthDependency:
    """`require_valid_api_key_or_gateway_token` -- the plain (non-SSE)
    sibling of `require_valid_api_key_or_gateway_token_sse`
    (`test_live_router.py`'s own `TestSseAuthCombined`), header-only: no
    query-param fallback, since a plain fetch()-based route (unlike
    EventSource) can always attach a header.
    """

    async def test_missing_everything_raises_401(self) -> None:
        app = create_app(settings=Settings(), redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request()
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token(request)
            assert exc_info.value.status_code == 401

    async def test_valid_api_key_is_still_accepted_with_no_gateway_token_configured(
        self,
    ) -> None:
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        app = create_app(settings=Settings(), redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-API-Key": API_KEY})
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token(
                request, header_key=API_KEY
            )  # must not raise

    async def test_unconfigured_gateway_token_does_not_bypass_the_api_key_floor(
        self,
    ) -> None:
        app = create_app(settings=Settings(), redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-CTTC-Token": "anything-at-all"})
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token(
                    request, header_token="anything-at-all"
                )
            assert exc_info.value.status_code == 401

    async def test_valid_gateway_token_is_accepted_with_no_api_key_at_all(self) -> None:
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-CTTC-Token": GATEWAY_TOKEN})
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token(
                request, header_token=GATEWAY_TOKEN
            )  # must not raise

    async def test_wrong_gateway_token_falls_back_to_a_valid_api_key(self) -> None:
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-CTTC-Token": "wrong", "X-API-Key": API_KEY})
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token(
                request, header_token="wrong", header_key=API_KEY
            )  # must not raise

    async def test_wrong_gateway_token_and_no_valid_api_key_raises_401(self) -> None:
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-CTTC-Token": "wrong"})
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token(request, header_token="wrong")
            assert exc_info.value.status_code == 401
