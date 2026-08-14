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
from log_sump_common.auth import RedisApiKeyAuthBackend
from log_sump_common.config import Settings
from redis.asyncio import Redis

from .buffers import BufferManager
from .scheduling import Scheduler
from .sessions import SessionManager

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


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
