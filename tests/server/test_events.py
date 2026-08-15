"""Migration plan Phase 5: EventManager -- a representative subset of a
prior gateway implementation's own extensive test_events.py, ported to
docker_host scoping.
"""

import json
import time
from datetime import UTC, datetime

import pytest
from fakeredis import FakeAsyncRedis

from log_sump.common.redis_keys import stream_key
from log_sump.common.schema import Kind
from log_sump.server.buffers import BufferManager
from log_sump.server.events import (
    Action,
    EventManager,
    InvalidEvent,
    LogCondition,
    MetricCondition,
    UnknownEvent,
)
from log_sump.server.sessions import SessionManager

DOCKER_HOST = "daemon-a"


def time_ms_now() -> float:
    return time.time() * 1000.0


def _manager() -> tuple[EventManager, FakeAsyncRedis]:
    redis = FakeAsyncRedis()
    buffers = BufferManager(redis)
    sessions = SessionManager(redis)
    return EventManager(redis, buffers, sessions), redis


async def _seed_metric(
    redis: FakeAsyncRedis, *, ts_ms: float, container_id: str, cpu_pct: float
) -> None:
    entry_id = f"{int(ts_ms)}-0"
    fields = {
        "kind": "metric",
        "docker_host": DOCKER_HOST,
        "container_name": container_id,
        "container_id": container_id,
        "ts": datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat(),
        "seq": 1,
        "metric_scope": "container",
        "cpu_pct": cpu_pct,
        "source": "docker stats",
    }
    stream = stream_key(DOCKER_HOST, Kind.METRIC)
    await redis.xadd(stream, {"data": json.dumps(fields)}, id=entry_id)


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


class TestValidation:
    async def test_unknown_metric_raises(self) -> None:
        manager, _redis = _manager()
        # `metric` is a Literal field -- "disk" is only reachable at
        # runtime (a real client sends JSON, not a typed dataclass), which
        # is exactly what _validate()'s own runtime check is for.
        bad_condition = MetricCondition(
            metric="disk",  # ty: ignore[invalid-argument-type]
            op=">",
            threshold=1,
        )
        action = Action(kind="snapshot", minutes=5)
        with pytest.raises(InvalidEvent):
            await manager.create("x", DOCKER_HOST, [bad_condition], action)

    async def test_invalid_regex_raises(self) -> None:
        manager, _redis = _manager()
        with pytest.raises(InvalidEvent):
            await manager.create(
                "x", DOCKER_HOST, [LogCondition(pattern="[")], Action(kind="snapshot", minutes=5)
            )

    async def test_snapshot_action_requires_minutes(self) -> None:
        manager, _redis = _manager()
        with pytest.raises(InvalidEvent):
            await manager.create(
                "x", DOCKER_HOST, [LogCondition(pattern="ERROR")], Action(kind="snapshot")
            )

    async def test_requires_at_least_one_condition(self) -> None:
        manager, _redis = _manager()
        with pytest.raises(InvalidEvent):
            await manager.create("x", DOCKER_HOST, [], Action(kind="snapshot", minutes=5))

    async def test_unknown_event_operations_raise(self) -> None:
        manager, _redis = _manager()
        with pytest.raises(UnknownEvent):
            manager.status_of("nope")
        with pytest.raises(UnknownEvent):
            manager.enable("nope")
        with pytest.raises(UnknownEvent):
            await manager.cancel("nope")


class TestMetricEvents:
    async def test_snapshot_action_starts_an_event_owned_buffer(self) -> None:
        manager, _redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        ev = manager._events[event_id]
        assert ev.buffer_id is not None
        assert manager._buffers._buffers[ev.buffer_id]["owned_by_event"] is True

    async def test_tick_fires_snapshot_when_threshold_crossed(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=50.0)
        await manager.tick(now=start_ts + 20.0)
        assert manager.status_of(event_id)["status"] == "armed"

        await _seed_metric(redis, ts_ms=start_ts + 30.0, container_id="api", cpu_pct=90.0)
        await manager.tick(now=start_ts + 40.0)
        status = manager.status_of(event_id)
        assert status["status"] == "triggered"
        assert status["artifact_id"] is not None
        assert "cpu=90.0" in status["trigger_detail"]

        data = await manager._sessions.download(status["artifact_id"])
        assert data[:2] == b"PK"  # zip magic

    async def test_does_not_refire_while_condition_stays_true(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=90.0)
        await manager.tick(now=start_ts + 20.0)
        first_artifact = manager.status_of(event_id)["artifact_id"]
        assert manager.status_of(event_id)["trigger_count"] == 1

        await manager.tick(now=start_ts + 30.0)  # still 90 -- latch stays closed
        status = manager.status_of(event_id)
        assert status["artifact_id"] == first_artifact
        assert status["trigger_count"] == 1

    async def test_reset_forces_an_immediate_refire(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=90.0)
        await manager.tick(now=start_ts + 20.0)
        first_artifact = manager.status_of(event_id)["artifact_id"]

        manager.reset(event_id)
        assert manager.status_of(event_id)["status"] == "armed"
        await manager.tick(now=start_ts + 30.0)  # still 90, but reset() re-armed the latch
        status = manager.status_of(event_id)
        assert status["artifact_id"] != first_artifact
        assert status["trigger_count"] == 2

    async def test_disabled_event_is_not_evaluated(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        manager.disable(event_id)
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=90.0)
        await manager.tick(now=start_ts + 20.0)
        assert manager.status_of(event_id)["trigger_count"] == 0

        manager.enable(event_id)
        await manager.tick(now=start_ts + 30.0)
        assert manager.status_of(event_id)["status"] == "triggered"

    async def test_recording_action_starts_a_forward_session(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "cpu high",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">=", threshold=80)],
            Action(kind="recording", duration_minutes=5),
        )
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=80.0)
        await manager.tick(now=start_ts + 20.0)
        status = manager.status_of(event_id)
        assert status["status"] == "triggered"
        assert manager._sessions.status_of(status["artifact_id"])["status"] == "running"


class TestLogEvents:
    async def test_tick_fires_on_regex_match_in_new_rows(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "errors",
            DOCKER_HOST,
            [LogCondition(pattern=r"ERROR")],
            Action(kind="snapshot", minutes=5),
        )
        start_ts = time_ms_now()
        await _seed_log(redis, ts_ms=start_ts + 10.0, container_id="web", message="all good")
        await manager.tick(now=start_ts + 20.0)
        assert manager.status_of(event_id)["status"] == "armed"

        await _seed_log(
            redis, ts_ms=start_ts + 30.0, container_id="web", message="ERROR: disk full"
        )
        await manager.tick(now=start_ts + 40.0)
        status = manager.status_of(event_id)
        assert status["status"] == "triggered"
        assert "ERROR: disk full" in status["trigger_detail"]

    async def test_pre_existing_backlog_does_not_count_as_a_fresh_match(self) -> None:
        manager, redis = _manager()
        start_ts = time_ms_now()
        await _seed_log(redis, ts_ms=start_ts + 10.0, container_id="web", message="ERROR: boom")
        event_id = await manager.create(
            "errors",
            DOCKER_HOST,
            [LogCondition(pattern=r"ERROR")],
            Action(kind="snapshot", minutes=5),
        )
        await manager.tick(now=start_ts + 20.0)
        assert manager.status_of(event_id)["status"] == "armed"


class TestMultipleConditions:
    async def test_match_all_requires_every_condition(self) -> None:
        manager, redis = _manager()
        event_id = await manager.create(
            "multi",
            DOCKER_HOST,
            [
                MetricCondition(metric="cpu", op=">", threshold=80),
                LogCondition(pattern="ERROR"),
            ],
            Action(kind="snapshot", minutes=5),
            match="all",
        )
        start_ts = time_ms_now()
        await _seed_metric(redis, ts_ms=start_ts + 10.0, container_id="api", cpu_pct=90.0)
        await manager.tick(now=start_ts + 20.0)
        assert manager.status_of(event_id)["status"] == "armed"

        await _seed_log(
            redis, ts_ms=start_ts + 30.0, container_id="web", message="ERROR: disk full"
        )
        await manager.tick(now=start_ts + 40.0)
        status = manager.status_of(event_id)
        assert status["status"] == "triggered"
        assert "cpu=90.0" in status["trigger_detail"]
        assert "ERROR: disk full" in status["trigger_detail"]


class TestUpdateAndCancel:
    async def test_update_action_from_snapshot_to_recording_stops_the_buffer(self) -> None:
        manager, _redis = _manager()
        event_id = await manager.create(
            "x",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        buffer_id = manager._events[event_id].buffer_id
        assert buffer_id in manager._buffers._buffers

        await manager.update(event_id, action=Action(kind="recording", duration_minutes=5))

        assert buffer_id not in manager._buffers._buffers
        assert manager._events[event_id].buffer_id is None

    async def test_cancel_removes_event_and_its_buffer(self) -> None:
        manager, _redis = _manager()
        event_id = await manager.create(
            "x",
            DOCKER_HOST,
            [MetricCondition(metric="cpu", op=">", threshold=80)],
            Action(kind="snapshot", minutes=5),
        )
        buffer_id = manager._events[event_id].buffer_id

        await manager.cancel(event_id)

        assert buffer_id not in manager._buffers._buffers
        with pytest.raises(UnknownEvent):
            manager.status_of(event_id)
