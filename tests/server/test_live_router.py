"""GET /events (SSE) -- migration plan Phase 6.

`sse_generator` is tested directly (bounded iteration), not through a live
HTTP round-trip: an indefinitely-running `StreamingResponse` body can't
practically be exercised end-to-end through `httpx`'s `ASGITransport` here
-- confirmed while building this, it buffers a response to completion
(status code included) before returning anything to the caller, which
never happens for a generator that runs forever. `require_valid_api_key_sse`
and `require_valid_api_key_or_gateway_token_sse` (the dependency actually
wired onto the route) are tested directly too, via a hand-built `Request`,
for the same reason.
"""

import asyncio
import json

import pytest
from fakeredis import FakeAsyncRedis
from fastapi import HTTPException, Request

from log_sump.common.config import GatewayConfig, Settings
from log_sump.common.redis_keys import auth_key
from log_sump.server.app import create_app
from log_sump.server.deps import (
    require_valid_api_key_or_gateway_token_sse,
    require_valid_api_key_sse,
)
from log_sump.server.routers.live import sse_generator

API_KEY = "live-key"


def _request(*, headers: dict[str, str] | None = None, query_string: bytes = b"") -> Request:
    scope = {
        "type": "http",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "query_string": query_string,
    }
    return Request(scope)


class TestSseGenerator:
    async def test_first_chunk_is_a_connected_comment(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        gen = sse_generator(queue, is_disconnected=lambda: _false())

        first = await anext(gen)

        assert first == b": connected\n\n"
        await gen.aclose()

    async def test_yields_published_event_as_data(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        await queue.put({"type": "update", "docker_host": "daemon-a"})
        gen = sse_generator(queue, is_disconnected=lambda: _false())

        await anext(gen)  # the initial "connected" comment
        chunk = await anext(gen)

        expected = json.dumps({"type": "update", "docker_host": "daemon-a"})
        assert chunk == f"data: {expected}\n\n".encode()
        await gen.aclose()

    async def test_yields_keepalive_on_timeout(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        gen = sse_generator(queue, is_disconnected=lambda: _false(), keepalive_interval=0.02)

        await anext(gen)  # "connected"
        chunk = await anext(gen)

        assert chunk == b": keepalive\n\n"
        await gen.aclose()

    async def test_stops_once_disconnected(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        gen = sse_generator(queue, is_disconnected=lambda: _true())

        await anext(gen)  # "connected" -- disconnection is only checked after
        with pytest.raises(StopAsyncIteration):
            await anext(gen)


async def _false() -> bool:
    return False


async def _true() -> bool:
    return True


class TestSseAuth:
    async def test_missing_key_raises_401(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await require_valid_api_key_sse(_request())
        assert exc_info.value.status_code == 401

    async def test_header_key_is_accepted(self) -> None:
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        app = create_app(settings=Settings(), redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-API-Key": API_KEY})
            request.scope["app"] = app
            # header_key is normally resolved by FastAPI's own dependency
            # injection (Security(_api_key_header)) before this function's
            # body ever runs -- calling it directly bypasses that, so it's
            # passed explicitly here, simulating what FastAPI would have
            # already extracted from the X-API-Key header.
            await require_valid_api_key_sse(request, header_key=API_KEY)  # must not raise

    async def test_query_param_key_is_accepted(self) -> None:
        """The one channel a browser EventSource actually has."""
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        app = create_app(settings=Settings(), redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(query_string=f"api_key={API_KEY}".encode())
            request.scope["app"] = app
            await require_valid_api_key_sse(request)  # must not raise

    async def test_unknown_key_raises_401(self) -> None:
        redis = FakeAsyncRedis()
        app = create_app(settings=Settings(), redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(query_string=b"api_key=not-a-real-key")
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_sse(request)
            assert exc_info.value.status_code == 401


GATEWAY_TOKEN = "gw-secret"


class TestSseAuthCombined:
    """`require_valid_api_key_or_gateway_token_sse` -- what /events actually
    uses. A gateway-token-only client (cttc's renderer) never holds a
    daemon-scoped API key at all, so /events must accept either credential;
    but a deployment that never configures a gateway token must keep the
    exact same "some valid API key required" floor
    `require_valid_api_key_sse` already enforced, not silently open up.
    """

    async def test_missing_everything_raises_401(self) -> None:
        app = create_app(settings=Settings(), redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request()
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token_sse(request)
            assert exc_info.value.status_code == 401

    async def test_valid_api_key_is_still_accepted_with_no_gateway_token_configured(self) -> None:
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        app = create_app(settings=Settings(), redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(query_string=f"api_key={API_KEY}".encode())
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token_sse(request)  # must not raise

    async def test_unconfigured_gateway_token_does_not_bypass_the_api_key_floor(self) -> None:
        """A deployment with no gateway.token set must not become
        unauthenticated just because this dependency also knows how to
        check one -- GatewayTokenAuthBackend.is_valid(None) on its own is
        permissive by design (see require_gateway_token's own docstring),
        which is exactly the behavior this must NOT inherit here.
        """
        app = create_app(settings=Settings(), redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(query_string=b"token=anything-at-all")
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token_sse(request)
            assert exc_info.value.status_code == 401

    async def test_valid_gateway_token_header_is_accepted_with_no_api_key_at_all(self) -> None:
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(headers={"X-CTTC-Token": GATEWAY_TOKEN})
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token_sse(request, header_token=GATEWAY_TOKEN)

    async def test_valid_gateway_token_query_param_is_accepted(self) -> None:
        """The one channel a browser EventSource actually has."""
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(query_string=f"token={GATEWAY_TOKEN}".encode())
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token_sse(request)  # must not raise

    async def test_wrong_gateway_token_falls_back_to_a_valid_api_key(self) -> None:
        redis = FakeAsyncRedis()
        await redis.sadd(auth_key(API_KEY), "daemon-a")
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=redis)
        async with app.router.lifespan_context(app):
            request = _request(query_string=f"token=wrong&api_key={API_KEY}".encode())
            request.scope["app"] = app
            await require_valid_api_key_or_gateway_token_sse(request)  # must not raise

    async def test_wrong_gateway_token_and_no_valid_api_key_raises_401(self) -> None:
        settings = Settings(gateway=GatewayConfig(token=GATEWAY_TOKEN))
        app = create_app(settings=settings, redis=FakeAsyncRedis())
        async with app.router.lifespan_context(app):
            request = _request(query_string=b"token=wrong")
            request.scope["app"] = app
            with pytest.raises(HTTPException) as exc_info:
                await require_valid_api_key_or_gateway_token_sse(request)
            assert exc_info.value.status_code == 401
