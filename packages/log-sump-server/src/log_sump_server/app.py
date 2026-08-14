"""FastAPI app factory + lifespan (spec §5.5, §7).

Starts the Redis Streams ingestion consumer and retention trimmer as
background tasks alongside the query API, since this process already holds
the persistent async Redis connection both need — see
`log_sump_server.ingest.consumer`/`.trimmer`'s module docstrings for why
they live here rather than in log-listener (the four-process constraint,
spec §3.2, doesn't allow a fifth).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import structlog
from fastapi import FastAPI
from log_sump_common.auth import RedisApiKeyAuthBackend
from log_sump_common.config import Settings, load_settings
from redis.asyncio import Redis

from .ingest.consumer import run_consumer
from .ingest.trimmer import run_trimmer
from .routers import admin, catalog, daemons, files, health, records, series

logger = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None, redis: Redis | None = None) -> FastAPI:
    """Build the app. `redis`, if given, is used as-is instead of building
    one from `settings.redis.url` — the seam that lets tests inject a
    `fakeredis.FakeAsyncRedis()` instead of needing a real connection. In
    that case ownership stays with the caller: this app won't close it on
    shutdown, matching how `Transport` is injected elsewhere in this
    project rather than constructed internally by whatever uses it.
    """
    settings = settings or load_settings()
    owns_redis = redis is None

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal redis
        if redis is None:
            redis = Redis.from_url(settings.redis.url)
        app.state.settings = settings
        app.state.redis = redis
        app.state.auth_backend = RedisApiKeyAuthBackend(redis)

        daemon_ids = [daemon.id for daemon in settings.daemons]
        consumer_task = asyncio.create_task(run_consumer(redis))
        trimmer_task = asyncio.create_task(
            run_trimmer(
                redis,
                daemon_ids,
                retention_days=settings.retention.retention_days,
                metrics_retention_days=settings.retention.effective_metrics_retention_days(),
                trim_interval_seconds=settings.retention.trim_interval_seconds,
            )
        )
        await logger.ainfo("server.starting", daemon_ids=daemon_ids)
        try:
            yield
        finally:
            for task in (consumer_task, trimmer_task):
                task.cancel()
            await asyncio.gather(consumer_task, trimmer_task, return_exceptions=True)
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
    return app


app = create_app()
