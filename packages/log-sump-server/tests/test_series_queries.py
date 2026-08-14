"""Phase 1 of the server migration: point/index_at/ticks/bucketed/find_text
in queries.py -- timeline queries the cttc scrubbing UI needs on top of
log-sump's existing per-daemon Streams. See the plan's Phase 1 for the
cttc equivalents each of these replaces.
"""

import json
from datetime import UTC, datetime

from fakeredis import FakeAsyncRedis
from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind, MetricRecord
from log_sump_server.queries import bucketed, find_text, index_at, point_at, ticks

DOCKER_HOST = "daemon-a"


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


async def _seed_log(
    redis: FakeAsyncRedis, *, ts: datetime, container_id: str, message: str
) -> str:
    entry_id = f"{_ms(ts)}-0"
    fields = {
        "kind": "log",
        "docker_host": DOCKER_HOST,
        "container_name": f"name-{container_id}",
        "container_id": container_id,
        "ts": ts.isoformat(),
        "seq": 1,
        "stream": "stdout",
        "level": "info",
        "message": message,
        "fields": {},
        "raw": message,
    }
    await redis.xadd(stream_key(DOCKER_HOST, Kind.LOG), {"data": json.dumps(fields)}, id=entry_id)
    return entry_id


async def _seed_metric(
    redis: FakeAsyncRedis,
    *,
    ts: datetime,
    container_id: str,
    cpu_pct: float | None = None,
    mem_pct: float | None = None,
    net_rx_bytes: int | None = None,
    net_tx_bytes: int | None = None,
) -> str:
    entry_id = f"{_ms(ts)}-0"
    fields = {
        "kind": "metric",
        "docker_host": DOCKER_HOST,
        "container_name": f"name-{container_id}",
        "container_id": container_id,
        "ts": ts.isoformat(),
        "seq": 1,
        "metric_scope": "container",
        "cpu_pct": cpu_pct,
        "mem_pct": mem_pct,
        "net_rx_bytes": net_rx_bytes,
        "net_tx_bytes": net_tx_bytes,
        "source": "docker stats",
    }
    stream = stream_key(DOCKER_HOST, Kind.METRIC)
    await redis.xadd(stream, {"data": json.dumps(fields)}, id=entry_id)
    return entry_id


async def test_point_at_returns_nearest_sample_per_container() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    # Seeded in chronological (== Stream ID) order -- a real stream can never
    # receive an XADD with an ID at or before its current top item, so out-
    # of-order seeding here would fail exactly like a real Redis would.
    await _seed_metric(redis, ts=t0, container_id="c1", cpu_pct=10.0)
    await _seed_metric(redis, ts=_dt(_ms(t0) + 1000), container_id="c2", cpu_pct=99.0)
    await _seed_metric(redis, ts=_dt(_ms(t0) + 5000), container_id="c1", cpu_pct=20.0)

    result = await point_at(redis, DOCKER_HOST, Kind.METRIC, _dt(_ms(t0) + 900))

    assert set(result) == {"c1", "c2"}
    _id_c1, record_c1 = result["c1"]
    assert isinstance(record_c1, MetricRecord)
    assert record_c1.cpu_pct == 10.0  # t0 (900ms away) closer than t0+5000ms
    _id_c2, record_c2 = result["c2"]
    assert isinstance(record_c2, MetricRecord)
    assert record_c2.cpu_pct == 99.0


async def test_index_at_finds_nearest_preceding_entry_for_container() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    id1 = await _seed_log(redis, ts=t0, container_id="c1", message="first")
    await _seed_log(redis, ts=_dt(_ms(t0) + 5000), container_id="c2", message="other-container")
    await _seed_log(redis, ts=_dt(_ms(t0) + 10000), container_id="c1", message="second")

    cursor = await index_at(redis, DOCKER_HOST, "c1", _dt(_ms(t0) + 2000))

    assert cursor == id1


async def test_index_at_falls_back_to_next_entry_when_nothing_precedes() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    id1 = await _seed_log(redis, ts=t0, container_id="c1", message="only-one")

    cursor = await index_at(redis, DOCKER_HOST, "c1", _dt(_ms(t0) - 5000))

    assert cursor == id1


async def test_ticks_counts_only_the_requested_container() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    t1 = _dt(_ms(t0) + 10000)
    await _seed_log(redis, ts=t0, container_id="c1", message="a")
    await _seed_log(redis, ts=_dt(_ms(t0) + 1000), container_id="c1", message="b")
    await _seed_log(redis, ts=_dt(_ms(t0) + 1500), container_id="c2", message="ignored")

    counts = await ticks(redis, DOCKER_HOST, "c1", Kind.LOG, t0, t1, px=10)

    assert sum(counts) == 2


async def test_bucketed_computes_max_per_pixel_and_net_rate_from_counters() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    t1 = _dt(_ms(t0) + 10000)
    # two samples 1s apart, total bytes 1000 -> 2000 => rate 1000 B/s
    await _seed_metric(
        redis, ts=t0, container_id="c1", cpu_pct=5.0, net_rx_bytes=500, net_tx_bytes=500
    )
    await _seed_metric(
        redis,
        ts=_dt(_ms(t0) + 1000),
        container_id="c1",
        cpu_pct=15.0,
        net_rx_bytes=1000,
        net_tx_bytes=1000,
    )

    out = await bucketed(redis, DOCKER_HOST, t0, t1, px=10)

    assert len(out) == 1
    entry = out[0]
    assert entry["container_id"] == "c1"
    assert max(v for v in entry["cpu"] if v is not None) == 15.0
    assert max(v for v in entry["net"] if v is not None) == 1000.0


async def test_bucketed_skips_rate_on_counter_reset() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    t1 = _dt(_ms(t0) + 10000)
    await _seed_metric(redis, ts=t0, container_id="c1", net_rx_bytes=5000, net_tx_bytes=0)
    await _seed_metric(
        redis, ts=_dt(_ms(t0) + 1000), container_id="c1", net_rx_bytes=100, net_tx_bytes=0
    )  # restart: counter dropped

    out = await bucketed(redis, DOCKER_HOST, t0, t1, px=10)

    assert all(v is None for v in out[0]["net"])


async def test_find_text_forward_and_wraparound() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    id_needle_early = await _seed_log(redis, ts=t0, container_id="c1", message="boot ok")
    await _seed_log(redis, ts=_dt(_ms(t0) + 1000), container_id="c1", message="running")
    id_needle_late = await _seed_log(
        redis, ts=_dt(_ms(t0) + 2000), container_id="c1", message="boot retry"
    )

    # starting search from the middle entry, forward, should find the later hit first
    from_middle = f"{_ms(t0) + 1000}-0"
    cursor = await find_text(redis, DOCKER_HOST, "c1", "boot", cursor=from_middle, forward=True)
    assert cursor == id_needle_late

    # starting from the last entry, forward, must wrap to the earliest hit
    from_end = f"{_ms(t0) + 2000}-0"
    cursor = await find_text(redis, DOCKER_HOST, "c1", "boot", cursor=from_end, forward=True)
    assert cursor == id_needle_early


async def test_find_text_ignores_other_containers() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    await _seed_log(redis, ts=t0, container_id="c2", message="needle")

    cursor = await find_text(redis, DOCKER_HOST, "c1", "needle", cursor=None, forward=True)

    assert cursor is None
