import asyncio
import json
from datetime import UTC, datetime

from log_sump_common.schema import MetricRecord, RecordAdapter
from log_sump_common.transport import ExecResult, TransportError
from log_sump_listener.container_stats import _parse_stats_line, run_container_stats
from log_sump_listener.registry import ContainerRef, Registry

from .conftest import FakeRecordsLogger, FakeTransport

RAW_ROW = {
    "ID": "c1",
    "Name": "web",
    "CPUPerc": "0.50%",
    "MemUsage": "50MiB / 4GiB",
    "MemPerc": "1.23%",
    "NetIO": "1.2kB / 3.4kB",
    "BlockIO": "0B / 0B",
    "PIDs": "5",
}


def test_parse_stats_line_normalizes_units() -> None:
    record = _parse_stats_line(
        docker_host="daemon-a", data=RAW_ROW, seq=1, ts=datetime(2026, 8, 14, tzinfo=UTC)
    )

    assert record.metric_scope == "container"
    assert record.cpu_pct == 0.5
    assert record.mem_used_bytes == 50 * 1024 * 1024
    assert record.mem_limit_bytes == 4 * 1024**3
    assert record.mem_pct == 1.23
    assert record.net_rx_bytes == 1200
    assert record.net_tx_bytes == 3400
    assert record.blk_read_bytes == 0
    assert record.blk_write_bytes == 0
    assert record.pids == 5
    assert record.source == "docker stats"
    assert record.raw == RAW_ROW


def test_parse_stats_line_missing_pids_is_none() -> None:
    row = dict(RAW_ROW)
    del row["PIDs"]
    record = _parse_stats_line(
        docker_host="daemon-a", data=row, seq=1, ts=datetime(2026, 8, 14, tzinfo=UTC)
    )
    assert record.pids is None


async def test_run_container_stats_only_emits_for_registry_known_containers() -> None:
    other_row = dict(RAW_ROW, ID="c2", Name="db")
    stdout = "\n".join([json.dumps(RAW_ROW), json.dumps(other_row)])
    transport = FakeTransport(run_result=ExecResult(returncode=0, stdout=stdout, stderr=""))
    registry = Registry()
    await registry.update("daemon-a", {ContainerRef(container_id="c1", container_name="web")})
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_container_stats("daemon-a", transport, registry, records_logger, stats_interval_s=10.0)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(records_logger.calls) == 1
    record = RecordAdapter.validate_json(records_logger.calls[0])
    assert isinstance(record, MetricRecord)
    assert record.container_id == "c1"


async def test_run_container_stats_survives_transport_errors() -> None:
    transport = FakeTransport(raise_on_start=TransportError("unreachable"))
    registry = Registry()
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_container_stats("daemon-a", transport, registry, records_logger, stats_interval_s=10.0)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert records_logger.calls == []
