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

import structlog
from log_sump_common.redis_keys import INGEST_LIST, stream_key
from log_sump_common.schema import RecordAdapter
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

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
) -> None:
    while True:
        try:
            await _consume_once(redis, poll_timeout_s=poll_timeout_s, batch_size=batch_size)
        except RedisError as exc:
            await logger.awarning("consumer.cycle_failed", error=str(exc))
            await asyncio.sleep(ERROR_BACKOFF_S)


async def _consume_once(redis: Redis, *, poll_timeout_s: float, batch_size: int) -> None:
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
    await _ingest_batch(redis, batch)


async def _ingest_batch(redis: Redis, batch: list[bytes | str | int]) -> None:
    pipe = redis.pipeline(transaction=False)
    queued = 0
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
        pipe.xadd(
            stream_key(record.docker_host, record.kind), {"data": RecordAdapter.dump_json(record)}
        )
        queued += 1
    if queued:
        await pipe.execute()
