"""FastAPI dependencies: settings, the shared async Redis client, and auth
(spec §9).

Two things read the same underlying `RedisApiKeyAuthBackend` answer
differently:

- `/catalog` and `/records` need the caller's *full* permitted-daemon set
  (to filter the catalog, or to check one specific `docker_host` query
  param against it) — `get_permitted_daemons`.
- The Redis inspection endpoint (redis_inspect.py) isn't scoped to any one
  daemon at all; it only needs to know the key is valid —
  `require_valid_api_key`.

Both raise 401 the same way on a missing/unknown key; only the shape of
what they return differs.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from redis.asyncio import Redis

from log_sump.common.auth import GatewayTokenAuthBackend, RedisApiKeyAuthBackend
from log_sump.common.config import Settings

from .broadcast import Broadcaster
from .buffers import BufferManager
from .events import EventManager
from .scheduling import Scheduler
from .sessions import SessionManager

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_gateway_token_header = APIKeyHeader(name="X-CTTC-Token", auto_error=False)


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_redis(request: Request) -> Redis:
    redis: Redis = request.app.state.redis
    return redis


async def get_permitted_daemons(
    request: Request,
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> frozenset[str]:
    if not api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing API key")
    auth_backend: RedisApiKeyAuthBackend = request.app.state.auth_backend
    permitted = await auth_backend.permitted_daemons(api_key)
    if permitted is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid API key")
    return permitted


async def require_valid_api_key(
    permitted: Annotated[frozenset[str], Security(get_permitted_daemons)],
) -> None:
    """Any authenticated key is enough — no per-daemon check.

    Used only by the read-only Redis inspection endpoint, which isn't
    scoped to a daemon's records at all (see redis_inspect.py's module
    docstring for why any valid key, not a separate elevated tier).
    """


async def require_valid_api_key_sse(
    request: Request,
    header_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> None:
    """Same "any valid key is enough" check as `require_valid_api_key`, but
    also accepts the key via an `api_key` query param — the one concession
    browsers force: a native `EventSource` (`GET /events`, `routers/
    live.py`) can't attach a custom header at all, by spec, so the query
    string is the only channel it has. `require_gateway_token` below has
    the identical `?token=` fallback, for the identical reason.
    """
    api_key = header_key or request.query_params.get("api_key")
    if not api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing API key")
    auth_backend: RedisApiKeyAuthBackend = request.app.state.auth_backend
    permitted = await auth_backend.permitted_daemons(api_key)
    if permitted is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid API key")


async def get_raw_api_key(
    permitted: Annotated[frozenset[str], Security(get_permitted_daemons)],
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> str:
    """The caller's own validated API key string — for a route (file
    upload) that needs to *grant* this key access to something new, not
    just check what it can already see (see local_upload.py). Depends on
    `get_permitted_daemons` first so an invalid/missing key still 401s the
    same way every other route does — `api_key` is guaranteed non-`None`
    by the time that dependency has already succeeded.
    """
    assert api_key is not None
    return api_key


def require_daemon_access(docker_host: str, permitted: frozenset[str]) -> None:
    """Plain helper (not a FastAPI dependency) — call from a route handler
    that already has `docker_host` as its own query/path param and
    `permitted` from `get_permitted_daemons`, once both are in hand.
    """
    if docker_host not in permitted:
        raise HTTPException(status.HTTP_403_FORBIDDEN, f"not permitted for daemon {docker_host!r}")


def get_session_manager(request: Request) -> SessionManager:
    manager: SessionManager = request.app.state.sessions
    return manager


def get_buffer_manager(request: Request) -> BufferManager:
    manager: BufferManager = request.app.state.buffers
    return manager


def get_scheduler(request: Request) -> Scheduler:
    scheduler: Scheduler = request.app.state.scheduler
    return scheduler


def get_event_manager(request: Request) -> EventManager:
    manager: EventManager = request.app.state.events
    return manager


def get_broadcaster(request: Request) -> Broadcaster:
    broadcaster: Broadcaster = request.app.state.broadcaster
    return broadcaster


async def require_gateway_token(
    request: Request,
    header_token: Annotated[str | None, Security(_gateway_token_header)] = None,
) -> None:
    """Gates a gateway-mesh/admin route behind the shared-secret gateway
    token (migration plan Phase 7) -- an ordinary per-route dependency (not
    a blanket middleware) to match this codebase's existing convention
    (every other auth tier here is a `Depends()`, not middleware; see this
    module's own docstring). Unlike `require_valid_api_key`, a missing/
    unset `GatewayTokenAuthBackend.token` means "no gate at all", not
    "reject" -- an unset token stays exactly as permissive as a deployment
    that never opted into requiring one, matching the embedded,
    never-network-reachable case.

    Accepts a `?token=` query param as a fallback alongside the header, for
    the same reason `require_valid_api_key_sse` does: a browser's native
    `EventSource`/plain navigation (`GET /mlog`'s download) can't always
    attach a custom header.
    """
    backend: GatewayTokenAuthBackend = request.app.state.gateway_token_backend
    presented = header_token or request.query_params.get("token")
    if not backend.is_valid(presented):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or incorrect X-CTTC-Token")
