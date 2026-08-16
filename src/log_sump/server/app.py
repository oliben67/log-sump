"""FastAPI app factory + lifespan (spec §5.5, §7).

Starts the Redis Streams ingestion consumer and retention trimmer as
background tasks alongside the query API, since this process already holds
the persistent async Redis connection both need — see
`log_sump.server.ingest.consumer`/`.trimmer`'s module docstrings for why
they live here rather than in log-listener (the four-process constraint,
spec §3.2, doesn't allow a fifth).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path

import structlog
from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from redis.asyncio import Redis

from log_sump.common.auth import GatewayTokenAuthBackend, RedisApiKeyAuthBackend
from log_sump.common.config import Settings, load_settings

from .broadcast import Broadcaster
from .buffers import BufferManager
from .events import EventManager
from .ingest.consumer import POLL_TIMEOUT_S, run_consumer
from .ingest.trimmer import run_trimmer
from .routers import admin, catalog, daemons, files, gateway, health, records, series
from .routers import buffers as buffers_router
from .routers import events as events_router
from .routers import live as live_router
from .routers import scheduling as scheduling_router
from .routers import sessions as sessions_router
from .routers import transforms as transforms_router
from .scheduling import Scheduler
from .sessions import SessionManager
from .transforms import TransformFn, TransformRegistry

logger = structlog.get_logger(__name__)


async def run_tick_loop(
    sessions: SessionManager,
    buffers: BufferManager,
    scheduler: Scheduler,
    *,
    interval_seconds: float,
) -> None:
    """Periodically ticks sessions/buffers/scheduler (migration plan
    Phase 4) -- the prior gateway implementation's own equivalent is
    referenced across recording_session.py/rolling_buffer.py's docstrings
    as "server.py's background loop"/"sessions_loop". One combined loop,
    not three separate tasks: these tick()s are cheap, in-memory-only
    sweeps: the actual Redis I/O only happens when a session/buffer's window is
    actually exported (on stop, or once a session's own duration elapses),
    not on every tick.
    """
    while True:
        await sessions.tick()
        buffers.tick()
        scheduler.tick()
        await asyncio.sleep(interval_seconds)


async def run_events_tick_loop(events: EventManager, *, interval_seconds: float) -> None:
    """Periodically ticks events.py's condition checks (migration plan
    Phase 5) -- its own loop, not folded into run_tick_loop above: unlike
    that loop's cheap in-memory sweeps, every tick here does a real Redis
    read per condition per enabled event.
    """
    while True:
        await events.tick()
        await asyncio.sleep(interval_seconds)


def create_app(
    settings: Settings | None = None,
    redis: Redis | None = None,
    extra_routers: list[APIRouter] | None = None,
) -> FastAPI:
    """Build the app. `redis`, if given, is used as-is instead of building
    one from `settings.redis.url` — the seam that lets tests inject a
    `fakeredis.FakeAsyncRedis()` instead of needing a real connection. In
    that case ownership stays with the caller: this app won't close it on
    shutdown, matching how `Transport` is injected elsewhere in this
    project rather than constructed internally by whatever uses it.

    `extra_routers` is log-sump's extension seam for a deployment that
    needs routes beyond the built-in API surface: an external project
    (e.g. log-sump-extended) builds its own `APIRouter`s and passes them
    in explicitly at construction, rather than log-sump discovering and
    importing anything itself. log-sump has no idea what routes it's
    carrying beyond its own -- no runtime directory scan, no dynamic
    import, no knowledge of any specific caller. Each router is
    additive-only: one that collides with a built-in (or an earlier extra
    router's) route at the same `(path, method)` is rejected whole rather
    than silently shadowing it (confirmed the hard way once already --
    see the collision-check below).
    """
    settings = settings or load_settings()
    owns_redis = redis is None

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal redis
        if redis is None:
            # socket_timeout must exceed POLL_TIMEOUT_S: this client is
            # shared with run_consumer's BLPOP below, and redis-py's own
            # default socket_timeout (5s) is <= a 5s BLPOP wait, so the
            # client-side read timeout was racing the server-side BLPOP
            # timeout and usually losing -- confirmed by booting a real
            # container and watching every idle poll cycle log a spurious
            # "Timeout reading from ..." instead of BLPOP's normal, quiet
            # "nothing arrived" nil reply.
            redis = Redis.from_url(settings.redis.url, socket_timeout=POLL_TIMEOUT_S + 15.0)
        app.state.settings = settings
        app.state.redis = redis
        app.state.auth_backend = RedisApiKeyAuthBackend(redis)
        app.state.gateway_token_backend = GatewayTokenAuthBackend(settings.gateway.token)
        app.state.sessions = SessionManager(redis)
        app.state.buffers = BufferManager(redis)
        app.state.scheduler = Scheduler(app.state.sessions)
        app.state.events = EventManager(redis, app.state.buffers, app.state.sessions)
        app.state.broadcaster = Broadcaster()

        transform_fns: list[tuple[str, TransformFn]] = []
        if settings.transforms.directory:
            registry = TransformRegistry(Path(settings.transforms.directory))
            app.state.transform_registry = registry
            if settings.transforms.active:
                transform_fns = registry.load(settings.transforms.active)
        else:
            app.state.transform_registry = None

        daemon_ids = [daemon.id for daemon in settings.daemons]
        consumer_task = asyncio.create_task(
            run_consumer(
                redis, transform_fns=transform_fns, broadcaster=app.state.broadcaster
            )
        )
        trimmer_task = asyncio.create_task(
            run_trimmer(
                redis,
                daemon_ids,
                retention_days=settings.retention.retention_days,
                metrics_retention_days=settings.retention.effective_metrics_retention_days(),
                trim_interval_seconds=settings.retention.trim_interval_seconds,
            )
        )
        tick_task = asyncio.create_task(
            run_tick_loop(
                app.state.sessions,
                app.state.buffers,
                app.state.scheduler,
                interval_seconds=settings.server.tick_interval_seconds,
            )
        )
        events_tick_task = asyncio.create_task(
            run_events_tick_loop(
                app.state.events, interval_seconds=settings.server.events_tick_interval_seconds
            )
        )
        await logger.ainfo("server.starting", daemon_ids=daemon_ids)
        try:
            yield
        finally:
            tasks = (consumer_task, trimmer_task, tick_task, events_tick_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if owns_redis:
                await redis.aclose()

    app = FastAPI(title="log-sump", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(catalog.router)
    app.include_router(records.router)
    app.include_router(admin.router)
    app.include_router(series.router)
    app.include_router(files.router)
    app.include_router(daemons.router)
    app.include_router(sessions_router.router)
    app.include_router(buffers_router.router)
    app.include_router(scheduling_router.router)
    app.include_router(events_router.router)
    app.include_router(transforms_router.router)
    app.include_router(live_router.router)
    app.include_router(gateway.router)
    if extra_routers:
        # An extra router is additive-only: it must never be able to make
        # one of log-sump's own advertised routes unreachable just by
        # declaring a route at the same (path, method) -- FastAPI resolves
        # overlapping routes in registration order, so a same-path extra
        # route registered after the built-in ones above would otherwise
        # shadow them silently (confirmed the hard way, back when this was
        # still a runtime-loaded plugin: one reproducing a legacy client's
        # own `/point`/`/index_at`/`/ticks`/`/series`/`/logs/find` paths --
        # names log-sump's own Phase-1 `series` router already used first
        # -- made those built-in routes unreachable until this check
        # caught it). A colliding router is rejected whole (not partially
        # mounted), and checked against every router mounted before it
        # (built-in or extra), not just the built-ins.
        known_routes = {
            (route.path, method)
            for route in app.routes
            if isinstance(route, APIRoute)
            for method in route.methods or ()
        }
        for extra_router in extra_routers:
            extra_routes = {
                (route.path, method)
                for route in extra_router.routes
                if isinstance(route, APIRoute)
                for method in route.methods or ()
            }
            conflicts = extra_routes & known_routes
            if conflicts:
                logger.error("extra_router.route_conflict", conflicts=sorted(conflicts))
                continue
            app.include_router(extra_router)
            known_routes |= extra_routes
    return app


app = create_app()
