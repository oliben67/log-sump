import time

from fakeredis import FakeAsyncRedis
from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind
from log_sump_server.ingest.trimmer import _trim_once


async def _xrange(redis: FakeAsyncRedis, key: str) -> list:
    entries = await redis.xrange(key)
    assert entries is not None
    return entries


async def test_trim_once_drops_entries_older_than_retention_and_keeps_recent() -> None:
    redis = FakeAsyncRedis()
    key = stream_key("daemon-a", Kind.LOG)

    now_ms = int(time.time() * 1000)
    old_ms = now_ms - 10 * 24 * 60 * 60 * 1000  # 10 days ago
    recent_ms = now_ms - 1 * 60 * 60 * 1000  # 1 hour ago

    await redis.xadd(key, {"data": "old"}, id=f"{old_ms}-0")
    await redis.xadd(key, {"data": "recent"}, id=f"{recent_ms}-0")

    await _trim_once(redis, ["daemon-a"], retention_days=7, metrics_retention_days=7)

    entries = await _xrange(redis, key)
    assert len(entries) == 1
    assert entries[0][1][b"data"] == b"recent"


async def test_trim_once_applies_independent_horizons_per_kind() -> None:
    redis = FakeAsyncRedis()
    log_key = stream_key("daemon-a", Kind.LOG)
    metric_key = stream_key("daemon-a", Kind.METRIC)

    now_ms = int(time.time() * 1000)
    three_days_ago = now_ms - 3 * 24 * 60 * 60 * 1000

    await redis.xadd(log_key, {"data": "log"}, id=f"{three_days_ago}-0")
    await redis.xadd(metric_key, {"data": "metric"}, id=f"{three_days_ago}-0")

    # logs kept for 7 days (3-day-old entry survives), metrics for 1 day
    # (3-day-old entry is trimmed) -- independently configurable per spec §7.
    await _trim_once(redis, ["daemon-a"], retention_days=7, metrics_retention_days=1)

    assert len(await _xrange(redis, log_key)) == 1
    assert len(await _xrange(redis, metric_key)) == 0


async def test_trim_once_sweeps_every_configured_daemon() -> None:
    redis = FakeAsyncRedis()
    now_ms = int(time.time() * 1000)
    old_ms = now_ms - 10 * 24 * 60 * 60 * 1000

    for daemon_id in ("daemon-a", "daemon-b"):
        await redis.xadd(stream_key(daemon_id, Kind.LOG), {"data": "old"}, id=f"{old_ms}-0")

    await _trim_once(redis, ["daemon-a", "daemon-b"], retention_days=7, metrics_retention_days=7)

    assert len(await _xrange(redis, stream_key("daemon-a", Kind.LOG))) == 0
    assert len(await _xrange(redis, stream_key("daemon-b", Kind.LOG))) == 0
