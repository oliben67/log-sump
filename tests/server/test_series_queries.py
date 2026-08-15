"""Phase 1 of the server migration: point/index_at/ticks/bucketed/find_text
in queries.py -- timeline queries the cttc scrubbing UI needs on top of
log-sump's existing per-daemon Streams. See the plan's Phase 1 for the
cttc equivalents each of these replaces.
"""

import json
from datetime import UTC, datetime

from fakeredis import FakeAsyncRedis

from log_sump.common.redis_keys import stream_key
from log_sump.common.schema import Kind, MetricRecord
from log_sump.server.queries import bucketed, find_text, index_at, latest_services, point_at, ticks

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
    container_name: str | None = None,
    cpu_pct: float | None = None,
    mem_pct: float | None = None,
    net_rx_bytes: int | None = None,
    net_tx_bytes: int | None = None,
) -> str:
    entry_id = f"{_ms(ts)}-0"
    fields = {
        "kind": "metric",
        "docker_host": DOCKER_HOST,
        "container_name": container_name or container_id,
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

    assert set(result) == {"c1", "c2"}  # no dot in either container_name -- own group, unmerged
    _id_c1, record_c1, is_service_c1 = result["c1"]
    assert isinstance(record_c1, MetricRecord)
    assert record_c1.cpu_pct == 10.0  # t0 (900ms away) closer than t0+5000ms
    assert is_service_c1 is False
    _id_c2, record_c2, _is_service_c2 = result["c2"]
    assert isinstance(record_c2, MetricRecord)
    assert record_c2.cpu_pct == 99.0


async def test_point_at_merges_swarm_task_instances_into_one_service() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    # Two different task instances of the same "web" service -- swarm-style
    # dotted names -- should collapse into one "web" group.
    await _seed_metric(redis, ts=t0, container_id="c1", container_name="web.1.aaa", cpu_pct=10.0)
    await _seed_metric(
        redis,
        ts=_dt(_ms(t0) + 1000),
        container_id="c2",
        container_name="web.2.bbb",
        cpu_pct=20.0,
    )

    result = await point_at(redis, DOCKER_HOST, Kind.METRIC, _dt(_ms(t0) + 900))

    assert set(result) == {"web"}
    _entry_id, record, is_service = result["web"]
    assert is_service is True
    assert record.container_id == "c2"  # t0+1000ms (100ms away) closer than t0's c1 (900ms away)


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
    assert entry["name"] == "c1"
    assert entry["ttype"] == "container"
    assert max(v for v in entry["cpu"] if v is not None) == 15.0
    assert max(v for v in entry["net"] if v is not None) == 1000.0


async def test_bucketed_merges_swarm_task_instances_via_max() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    t1 = _dt(_ms(t0) + 10000)
    # Two task instances of the same service -- each has its own counters
    # (a different container's cumulative bytes are unrelated, so no rate
    # is computed across them), but their cpu% should still max-merge into
    # one "web" series.
    await _seed_metric(
        redis, ts=t0, container_id="c1", container_name="web.1.aaa", cpu_pct=10.0
    )
    await _seed_metric(
        redis,
        ts=_dt(_ms(t0) + 1000),
        container_id="c2",
        container_name="web.2.bbb",
        cpu_pct=30.0,
    )

    out = await bucketed(redis, DOCKER_HOST, t0, t1, px=10)

    assert len(out) == 1
    entry = out[0]
    assert entry["name"] == "web"
    assert entry["ttype"] == "service"
    assert max(v for v in entry["cpu"] if v is not None) == 30.0


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


async def _seed_service(
    redis: FakeAsyncRedis, *, ts: datetime, seq: int, svc_id: str, name: str, replicas: str
) -> str:
    entry_id = f"{_ms(ts)}-{seq}"
    fields = {
        "kind": "service",
        "docker_host": DOCKER_HOST,
        "ts": ts.isoformat(),
        "seq": seq,
        "id": svc_id,
        "name": name,
        "replicas": replicas,
    }
    stream = stream_key(DOCKER_HOST, Kind.SERVICE)
    await redis.xadd(stream, {"data": json.dumps(fields)}, id=entry_id)
    return entry_id


async def test_latest_services_returns_only_the_newest_cycle() -> None:
    redis = FakeAsyncRedis()
    t0 = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)
    # Stale cycle: one service, later replaced by a fresh cycle of two.
    await _seed_service(redis, ts=t0, seq=1, svc_id="s0", name="stale", replicas="1/1")
    t1 = _dt(_ms(t0) + 5000)
    await _seed_service(redis, ts=t1, seq=1, svc_id="s1", name="web", replicas="3/3")
    await _seed_service(redis, ts=t1, seq=2, svc_id="s2", name="db", replicas="1/1")

    services = await latest_services(redis, DOCKER_HOST)

    assert {s.name for s in services} == {"web", "db"}


async def test_latest_services_empty_when_never_shipped() -> None:
    redis = FakeAsyncRedis()
    assert await latest_services(redis, DOCKER_HOST) == []
