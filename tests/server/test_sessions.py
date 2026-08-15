"""Migration plan Phase 4: SessionManager -- direct translation of cttc's
own recording_session.py, scoped to a docker_host instead of an arbitrary
open-source set.
"""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from fakeredis import FakeAsyncRedis

from log_sump.common.cttc_archive import read_archive
from log_sump.common.redis_keys import session_data_key, stream_key
from log_sump.common.schema import Kind
from log_sump.server.sessions import SessionManager, UnknownSession

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


async def test_session_status_of_unknown_session_raises() -> None:
    manager = SessionManager(FakeAsyncRedis())
    with pytest.raises(UnknownSession):
        manager.status_of("rec999")


async def test_session_starts_running_and_reports_status() -> None:
    manager = SessionManager(FakeAsyncRedis())
    sid = manager.start(DOCKER_HOST)

    status = manager.status_of(sid)

    assert status == {"session_id": sid, "status": "running", "ready": False, "safe": False}


async def test_session_stop_finishes_and_produces_downloadable_archive() -> None:
    redis = FakeAsyncRedis()
    manager = SessionManager(redis)
    sid = manager.start(DOCKER_HOST)
    start_ts = manager._sessions[sid].start_ts

    # Must land inside [start_ts, stop-time] -- stop() uses "now" as the
    # window's end, so this needs a real, current-ish timestamp, not an
    # arbitrary constant. The sleep guarantees real wall-clock time has
    # actually advanced past the entry's own offset before stop() closes
    # the window.
    await _seed_log(redis, ts_ms=start_ts + 10.0, container_id="c1", message="hello from session")
    await asyncio.sleep(0.05)

    await manager.stop(sid)

    status = manager.status_of(sid)
    assert status["status"] == "completed"
    assert status["ready"] is True

    data = await manager.download(sid)
    sources = read_archive(data)
    assert len(sources) == 1
    assert sources[0].log_rows[0].text == "hello from session"


async def test_session_stop_is_idempotent() -> None:
    redis = FakeAsyncRedis()
    manager = SessionManager(redis)
    sid = manager.start(DOCKER_HOST)

    await manager.stop(sid)
    first_data = await manager.download(sid)
    await manager.stop(sid)  # no-op, already completed
    second_data = await manager.download(sid)

    assert first_data == second_data


async def test_download_before_completion_raises_unknown_session() -> None:
    manager = SessionManager(FakeAsyncRedis())
    sid = manager.start(DOCKER_HOST)

    with pytest.raises(UnknownSession):
        await manager.download(sid)


async def test_tick_finishes_session_once_duration_elapses() -> None:
    redis = FakeAsyncRedis()
    manager = SessionManager(redis)
    sid = manager.start(DOCKER_HOST, duration_minutes=1.0)

    await manager.tick(now=manager._sessions[sid].start_ts + 30_000.0)  # 30s in: not yet
    assert manager.status_of(sid)["status"] == "running"

    await manager.tick(now=manager._sessions[sid].start_ts + 61_000.0)  # 61s in: done
    assert manager.status_of(sid)["status"] == "completed"


async def test_mark_safe_uses_its_own_ttl_instead_of_default() -> None:
    redis = FakeAsyncRedis()
    manager = SessionManager(redis)
    manager.default_ttl_seconds = 100.0
    sid = manager.start(DOCKER_HOST)
    manager.mark_safe(sid, max_keep_seconds=99999.0)

    await manager.stop(sid)

    ttl = await redis.ttl(session_data_key(sid))
    assert ttl > 100  # would be <=100 if the (much shorter) default had been used


async def test_sweep_drops_expired_completed_session_metadata() -> None:
    redis = FakeAsyncRedis()
    manager = SessionManager(redis)
    manager.default_ttl_seconds = 1.0
    sid = manager.start(DOCKER_HOST)
    await manager.stop(sid)
    assert sid in manager._sessions
    stored_ts = manager._sessions[sid].stored_ts
    assert stored_ts is not None

    await manager.tick(now=stored_ts + 2_000.0)

    assert sid not in manager._sessions
    with pytest.raises(UnknownSession):
        manager.status_of(sid)
