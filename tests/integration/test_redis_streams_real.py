"""Exercises the ingestion consumer and retention trimmer against a real
Redis (via `docker-compose.dev.yml`) rather than `fakeredis`.

Worth having as a real, separate tier: `fakeredis`'s async client has at
least one confirmed bug (cancelling a task blocked inside `BLPOP` wipes its
entire in-memory dataset, even unrelated keys — see the note in
`tests/server/test_consumer.py`), so the unit-test tier
alone doesn't prove `XADD`/`XRANGE`/`XTRIM MINID` behave correctly against
genuine Redis semantics.

Run with `task dev:up` first, then `task test:integration`.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest
from redis.asyncio import Redis

from log_sump.common.redis_keys import INGEST_LIST, stream_key
from log_sump.common.schema import Kind
from log_sump.server.ingest.consumer import run_consumer
from log_sump.server.ingest.trimmer import _trim_once

pytestmark = pytest.mark.integration

REDIS_URL = "redis://127.0.0.1:6379/0"

LOG_RECORD_JSON = (
    '{"kind":"log","docker_host":"integration-daemon","container_name":"web",'
    '"container_id":"c1","ts":"2026-08-14T12:00:00Z","seq":1,"stream":"stdout",'
    '"level":"info","message":"hello from real redis","fields":{},"raw":"hello"}'
)


@pytest.fixture
async def redis() -> Redis:
    client = Redis.from_url(REDIS_URL)
    await client.ping()  # fails fast with a clear error if dev:up wasn't run
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


async def test_consumer_moves_ingest_list_entries_into_a_real_stream(redis: Redis) -> None:
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON)

    task = asyncio.create_task(run_consumer(redis, poll_timeout_s=0.2))
    try:
        async with asyncio.timeout(5.0):
            # Polling external Redis state, not our own code's state -- an
            # asyncio.Event has no producer to set() here.
            while await redis.llen(INGEST_LIST) != 0:  # noqa: ASYNC110
                await asyncio.sleep(0.05)
            entries = await redis.xrange(stream_key("integration-daemon", Kind.LOG))
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert len(entries) == 1
    _id, fields = entries[0]
    assert b"hello from real redis" in fields[b"data"]


async def test_xtrim_minid_removes_old_entries_on_real_redis(redis: Redis) -> None:
    key = stream_key("integration-daemon", Kind.LOG)
    now_ms = int(time.time() * 1000)
    old_ms = now_ms - 10 * 24 * 60 * 60 * 1000
    recent_ms = now_ms - 60 * 1000

    await redis.xadd(key, {"data": "old"}, id=f"{old_ms}-0")
    await redis.xadd(key, {"data": "recent"}, id=f"{recent_ms}-0")

    await _trim_once(redis, ["integration-daemon"], retention_days=7, metrics_retention_days=7)

    entries = await redis.xrange(key)
    assert [f[b"data"] for _id, f in entries] == [b"recent"]
