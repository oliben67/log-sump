from datetime import UTC, datetime
from typing import Any

from log_sump_common.schema import (
    SYSTEM_SCOPE_ID,
    Kind,
    LogRecord,
    MetricRecord,
    RecordAdapter,
    ServiceRecord,
)


def _base_kwargs() -> dict[str, Any]:
    return dict(
        docker_host="daemon-a",
        container_name="web-1",
        container_id="abc123",
        ts=datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC),
        seq=1,
    )


def test_log_record_round_trips_through_json() -> None:
    record = LogRecord(
        **_base_kwargs(),
        stream="stdout",
        level="info",
        message="hello world",
        fields={"trace_id": "xyz"},
        raw='{"level":"info","message":"hello world","trace_id":"xyz"}',
    )
    payload = RecordAdapter.dump_json(record)
    restored = RecordAdapter.validate_json(payload)

    assert isinstance(restored, LogRecord)
    assert restored.kind == Kind.LOG
    assert restored.message == "hello world"
    assert restored.fields == {"trace_id": "xyz"}


def test_metric_record_round_trips_through_json() -> None:
    record = MetricRecord(
        **_base_kwargs(),
        metric_scope="container",
        cpu_pct=12.5,
        mem_used_bytes=1024,
        mem_limit_bytes=2048,
        mem_pct=50.0,
        net_rx_bytes=10,
        net_tx_bytes=20,
        pids=4,
        source="docker stats",
        raw={"CPUPerc": "12.50%"},
    )
    payload = RecordAdapter.dump_json(record)
    restored = RecordAdapter.validate_json(payload)

    assert isinstance(restored, MetricRecord)
    assert restored.kind == Kind.METRIC
    assert restored.cpu_pct == 12.5
    assert restored.blk_read_bytes is None  # optional per spec §11.7


def test_system_metric_uses_system_scope_id() -> None:
    record = MetricRecord(
        docker_host="daemon-a",
        container_name=SYSTEM_SCOPE_ID,
        container_id=SYSTEM_SCOPE_ID,
        ts=datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC),
        seq=1,
        metric_scope="system",
        source="/proc",
    )
    assert record.container_id == "__system__"
    assert record.container_name == "__system__"


def test_kind_discriminator_picks_correct_variant() -> None:
    log_json = (
        '{"kind":"log","docker_host":"d","container_name":"c","container_id":"c1",'
        '"ts":"2026-08-14T12:00:00Z","seq":1,"stream":"stdout","level":"info",'
        '"message":"hi","raw":"hi"}'
    )
    metric_json = (
        '{"kind":"metric","docker_host":"d","container_name":"c","container_id":"c1",'
        '"ts":"2026-08-14T12:00:00Z","seq":1,"metric_scope":"container","source":"docker stats"}'
    )
    service_json = (
        '{"kind":"service","docker_host":"d","ts":"2026-08-14T12:00:00Z","seq":1,'
        '"id":"s1","name":"web","replicas":"3/3"}'
    )
    assert isinstance(RecordAdapter.validate_json(log_json), LogRecord)
    assert isinstance(RecordAdapter.validate_json(metric_json), MetricRecord)
    assert isinstance(RecordAdapter.validate_json(service_json), ServiceRecord)


def test_service_record_round_trips_through_json() -> None:
    record = ServiceRecord(
        docker_host="daemon-a",
        ts=datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC),
        seq=1,
        id="s1abc",
        name="web",
        replicas="3/3",
    )
    payload = RecordAdapter.dump_json(record)
    restored = RecordAdapter.validate_json(payload)

    assert isinstance(restored, ServiceRecord)
    assert restored.kind == Kind.SERVICE
    assert restored.name == "web"
    assert restored.replicas == "3/3"
