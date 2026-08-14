"""Retention trimmer (spec §7): periodically `XTRIM MINID` every per-daemon
stream down to its kind's configured retention horizon — `retention_days`
for `:log` streams, `metrics_retention_days` for `:metric` streams,
independently. Runs against every *configured* daemon (not just currently
enabled ones), so a daemon that gets disabled still has its historical data
aged out on schedule instead of being retained forever.

Trims **exactly** (no `~`/`approximate`), confirmed against a real Redis
(not `fakeredis`, which didn't catch this): approximate `MINID` trimming is
only a hint, and Redis defers the actual delete until enough entries have
piled up past the boundary to make a bulk radix-tree-node removal worth it
— against a real server, two entries with one past the retention horizon
were *not* trimmed at all with `approximate=True`, silently breaking the
retention guarantee for low/moderate-volume streams. Unlike `MAXLEN`
trimming (which needs the stream's total length), `MINID` trimming is
already an amortized walk of just the entries being removed, not a full
scan — so exactness here doesn't cost what approximate trimming was
originally meant to save.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

import structlog
from log_sump_common.daemon_registry import list_registered_daemons
from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = structlog.get_logger(__name__)

_MS_PER_DAY = 24 * 60 * 60 * 1000


async def run_trimmer(
    redis: Redis,
    daemon_ids: Sequence[str],
    *,
    retention_days: int,
    metrics_retention_days: int,
    trim_interval_seconds: float,
) -> None:
    """`daemon_ids` is the YAML-configured baseline; a daemon registered at
    runtime since (migration plan Phase 3, `POST /daemons`) is re-read from
    the registry every cycle here rather than once at startup -- this
    background task already lives in log-server, which has unscoped Redis
    access to begin with, so merging the two on each tick costs nothing
    architecturally, unlike the listener side's own "how does it even find
    out" problem (see `daemon_registry.py`'s module docstring).
    """
    while True:
        current_ids = set(daemon_ids)
        try:
            current_ids.update(daemon.id for daemon in await list_registered_daemons(redis))
        except RedisError as exc:
            await logger.awarning("trimmer.registry_poll_failed", error=str(exc))
        await _trim_once(
            redis,
            sorted(current_ids),
            retention_days=retention_days,
            metrics_retention_days=metrics_retention_days,
        )
        await asyncio.sleep(trim_interval_seconds)


async def _trim_once(
    redis: Redis,
    daemon_ids: Sequence[str],
    *,
    retention_days: int,
    metrics_retention_days: int,
) -> None:
    now_ms = int(time.time() * 1000)
    log_minid = now_ms - retention_days * _MS_PER_DAY
    metric_minid = now_ms - metrics_retention_days * _MS_PER_DAY

    for docker_host in daemon_ids:
        await _trim_stream(redis, docker_host, Kind.LOG, log_minid)
        await _trim_stream(redis, docker_host, Kind.METRIC, metric_minid)
        # :service carries no metrics of its own retention_days config
        # (migration plan Phase 1b) -- reused since it's operational/
        # discovery data, closer in spirit to logs than to metrics.
        await _trim_stream(redis, docker_host, Kind.SERVICE, log_minid)


async def _trim_stream(redis: Redis, docker_host: str, kind: Kind, minid_ms: int) -> None:
    try:
        await redis.xtrim(stream_key(docker_host, kind), minid=minid_ms, approximate=False)
    except RedisError as exc:
        # One unreachable/misbehaving stream must not stop the rest of the
        # sweep or the next cycle -- retention is best-effort, not fatal.
        await logger.awarning(
            "trimmer.trim_failed", docker_host=docker_host, kind=kind.value, error=str(exc)
        )
