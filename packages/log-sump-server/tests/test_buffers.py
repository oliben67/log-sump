"""Migration plan Phase 4: BufferManager -- direct translation of cttc's
own rolling_buffer.py, scoped to a docker_host.
"""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from fakeredis import FakeAsyncRedis
from log_sump_common.cttc_archive import read_archive
from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind
from log_sump_server.buffers import MAX_OPEN, BufferManager, TooManyBuffers, UnknownBuffer

DOCKER_HOST = "daemon-a"


async def _seed_log(
    redis: FakeAsyncRedis, *, ts_ms: float, container_id: str, message: str
) -> None:
    entry_id = f"{int(ts_ms)}-0"
    fields = {
        "kind": "log",
        "docker_host": DOCKER_HOST,
        "container_name": container_id,
        "container_id": container_id,
        "ts": datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat(),
        "seq": 1,
        "stream": "stdout",
        "level": "info",
        "message": message,
        "fields": {},
        "raw": message,
    }
    await redis.xadd(stream_key(DOCKER_HOST, Kind.LOG), {"data": json.dumps(fields)}, id=entry_id)


async def test_pause_unknown_buffer_raises() -> None:
    manager = BufferManager(FakeAsyncRedis())
    with pytest.raises(UnknownBuffer):
        manager.pause("b999")


async def test_stop_returns_downloadable_archive_of_seeded_window() -> None:
    redis = FakeAsyncRedis()
    manager = BufferManager(redis)
    buffer_id = manager.start(DOCKER_HOST, minutes=5.0)
    start_ts = manager._buffers[buffer_id]["start_ts"]

    await _seed_log(redis, ts_ms=start_ts + 10.0, container_id="c1", message="buffered line")
    await asyncio.sleep(0.05)

    data = await manager.stop(buffer_id)

    sources = read_archive(data)
    assert len(sources) == 1
    assert sources[0].log_rows[0].text == "buffered line"
    with pytest.raises(UnknownBuffer):
        manager.pause(buffer_id)  # stop() removed it


async def test_pause_freezes_window_end() -> None:
    redis = FakeAsyncRedis()
    manager = BufferManager(redis)
    buffer_id = manager.start(DOCKER_HOST, minutes=5.0)
    start_ts = manager._buffers[buffer_id]["start_ts"]

    await _seed_log(redis, ts_ms=start_ts + 10.0, container_id="c1", message="before pause")
    await asyncio.sleep(0.05)
    manager.pause(buffer_id)
    paused_at = manager._buffers[buffer_id]["paused_at"]
    assert paused_at is not None

    await asyncio.sleep(0.05)
    await _seed_log(redis, ts_ms=paused_at + 10_000.0, container_id="c1", message="after pause")

    data = await manager.stop(buffer_id)
    sources = read_archive(data)
    texts = {row.text for row in sources[0].log_rows}
    assert texts == {"before pause"}  # the post-pause entry must not appear


async def test_snapshot_leaves_buffer_running() -> None:
    redis = FakeAsyncRedis()
    manager = BufferManager(redis)
    buffer_id = manager.start(DOCKER_HOST, minutes=5.0)

    await manager.snapshot(buffer_id)

    assert buffer_id in manager._buffers  # still open, unlike stop()
    await manager.stop(buffer_id)


async def test_tick_reclaims_stale_ad_hoc_buffer() -> None:
    manager = BufferManager(FakeAsyncRedis())
    buffer_id = manager.start(DOCKER_HOST, minutes=5.0)
    start_ts = manager._buffers[buffer_id]["start_ts"]

    reclaimed = manager.tick(now=start_ts + 25 * 3600 * 1000.0)  # >24h later

    assert reclaimed == [buffer_id]
    assert buffer_id not in manager._buffers


async def test_start_raises_when_too_many_buffers_open() -> None:
    manager = BufferManager(FakeAsyncRedis())
    for _ in range(MAX_OPEN):
        manager.start(DOCKER_HOST, minutes=1.0)

    with pytest.raises(TooManyBuffers):
        manager.start(DOCKER_HOST, minutes=1.0)
