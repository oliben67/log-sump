"""Ingestion consumer (spec §7): moves entries from Logstash's Redis list
output into per-daemon, per-kind Redis Streams.

Logstash's `redis` output can only `RPUSH` to a list — it has no native
`XADD` output — so this consumer bridges the gap: it drains
`redis_keys.INGEST_LIST`, validates each entry against the shared `Record`
schema, and `XADD`s it into the correct stream (`redis_keys.stream_key`),
batched through a pipeline for throughput. Malformed entries are logged and
dropped here — this is where Logstash's own "enforce schema, drop/flag
malformed" requirement (spec §5.3) actually gets implemented, so the
validation logic lives exactly once rather than being duplicated in the
Logstash filter config.

Each stream entry stores the record as one field (`{"data": <json>}`) rather
than spreading the schema across individual stream fields — `fields`/
`system`/`raw` are nested objects that would need their own JSON
sub-encoding regardless, and log-server is the only consumer that reads
these back (via `RecordAdapter.validate_json` on that one field), so there's
no benefit to a wider field layout for a reader that never existed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import structlog
from log_sump_common.redis_keys import INGEST_LIST
from log_sump_common.schema import LogRecord, RecordAdapter
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ..broadcast import Broadcaster
from ..transforms import TransformFn, apply_transforms
from .write import queue_record

logger = structlog.get_logger(__name__)

#: How many entries to drain per LPOP batch after the initial BLPOP wakeup.
BATCH_SIZE = 200
#: How long BLPOP waits for the first entry before looping again — keeps the
#: loop responsive to cancellation instead of blocking indefinitely.
POLL_TIMEOUT_S = 5.0
#: Backoff after a Redis-level failure, so a down Redis doesn't turn this
#: into a tight retry loop.
ERROR_BACKOFF_S = 2.0


async def run_consumer(
    redis: Redis,
    *,
    poll_timeout_s: float = POLL_TIMEOUT_S,
    batch_size: int = BATCH_SIZE,
    transform_fns: Sequence[tuple[str, TransformFn]] = (),
    broadcaster: Broadcaster | None = None,
) -> None:
    """`transform_fns` (migration plan Phase 5, `transforms.py`), if given,
    is applied to every incoming `LogRecord` immediately before this
    consumer's own validate-then-`XADD` step -- see `transforms.
    apply_transforms`'s docstring. Never applied to `MetricRecord`/
    `ServiceRecord`, matching cttc's own transform system (`LogSource`
    only, never `StatsSource`).

    `broadcaster` (migration plan Phase 6, `broadcast.py`), if given,
    publishes one `{"type": "update", "docker_host": ...}` SSE notification
    per distinct `docker_host` that received at least one record in a
    successfully-written batch -- cttc's own `broadcast({"type": "update",
    "source": ...})`, at daemon granularity (log-sump's own addressing
    unit throughout this migration) rather than per opened source.
    """
    while True:
        try:
            await _consume_once(
                redis,
                poll_timeout_s=poll_timeout_s,
                batch_size=batch_size,
                transform_fns=transform_fns,
                broadcaster=broadcaster,
            )
        except RedisError as exc:
            await logger.awarning("consumer.cycle_failed", error=str(exc))
            await asyncio.sleep(ERROR_BACKOFF_S)


async def _consume_once(
    redis: Redis,
    *,
    poll_timeout_s: float,
    batch_size: int,
    transform_fns: Sequence[tuple[str, TransformFn]] = (),
    broadcaster: Broadcaster | None = None,
) -> None:
    popped = await redis.blpop([INGEST_LIST], timeout=poll_timeout_s)
    if popped is None:
        return  # nothing arrived within the poll window -- loop and wait again

    _, first_item = popped
    batch = [first_item]
    if batch_size > 1:
        # LPOP ... COUNT atomically drains up to N more without blocking.
        rest = await redis.lpop(INGEST_LIST, batch_size - 1)
        if rest:
            batch.extend(rest)
    await _ingest_batch(redis, batch, transform_fns=transform_fns, broadcaster=broadcaster)


async def _ingest_batch(
    redis: Redis,
    batch: list[bytes | str | int],
    *,
    transform_fns: Sequence[tuple[str, TransformFn]] = (),
    broadcaster: Broadcaster | None = None,
) -> None:
    pipe = redis.pipeline(transaction=False)
    queued = 0
    touched_hosts: set[str] = set()
    for raw in batch:
        if not isinstance(raw, bytes | str):
            # redis-py's stub allows int (shared with other commands'
            # response types); LPOP/BLPOP on this list never actually
            # produces one, but skip rather than pass it to validate_json.
            continue
        try:
            record = RecordAdapter.validate_json(raw)
        except ValidationError as exc:
            await logger.awarning("consumer.malformed_record", error=str(exc))
            continue
        if transform_fns and isinstance(record, LogRecord):
            for transformed in apply_transforms(record, list(transform_fns)):
                queue_record(pipe, transformed)
                queued += 1
                touched_hosts.add(transformed.docker_host)
            continue
        queue_record(pipe, record)
        queued += 1
        touched_hosts.add(record.docker_host)
    if queued:
        await pipe.execute()
        if broadcaster is not None:
            for docker_host in touched_hosts:
                broadcaster.publish({"type": "update", "docker_host": docker_host})
