import asyncio
import json

from log_sump.common.schema import SYSTEM_SCOPE_ID, MetricRecord, RecordAdapter
from log_sump.common.transport import ExecResult, TransportError
from log_sump.listener import system_stats as ss
from log_sump.listener.system_stats import (
    _cpu_percent,
    _CpuSample,
    _parse_cpu_total_idle,
    _parse_diskstats,
    _parse_meminfo,
    _parse_net_dev,
    _split_proc_sections,
    _try_sample_container_aggregate,
    _try_sample_docker_disk_usage,
    run_system_stats,
)

from .conftest import FakeRecordsLogger, FakeTransport

STAT_TEXT_CYCLE_1 = "cpu  100 20 30 800 50 5 3 0 0 0\nintr 12345\n"
STAT_TEXT_CYCLE_2 = "cpu  110 20 30 850 55 5 3 0 0 0\nintr 12400\n"

MEMINFO_TEXT = (
    "MemTotal:       16384000 kB\n"
    "MemFree:         2000000 kB\n"
    "MemAvailable:    8000000 kB\n"
    "Buffers:          100000 kB\n"
)

NETDEV_TEXT = (
    "Inter-|   Receive                                                |  Transmit\n"
    " face |bytes    packets errs drop fifo frame compressed multicast|"
    "bytes    packets errs drop fifo colls carrier compressed\n"
    "    lo: 1234567      10    0    0    0     0          0         0"
    "  1234567      10    0    0    0     0       0          0\n"
    "  eth0: 5000000     100    0    0    0     0          0         0"
    "  3000000      80    0    0    0     0       0          0\n"
)

DISKSTATS_TEXT = (
    "   8       0 sda 1000 5 20000 100 2000 10 40000 200 0 300 300 0 0 0 0 0\n"
    "   8       1 sda1 500 2 10000 50 1000 5 20000 100 0 150 150 0 0 0 0 0\n"
    " 259       0 nvme0n1 3000 10 60000 150 4000 20 80000 250 0 400 400 0 0 0 0 0\n"
    " 259       1 nvme0n1p1 1500 5 30000 75 2000 10 40000 125 0 200 200 0 0 0 0 0\n"
    "   7       0 loop0 10 0 200 5 0 0 0 0 0 5 5 0 0 0 0 0\n"
)


def test_parse_cpu_total_idle() -> None:
    assert _parse_cpu_total_idle(STAT_TEXT_CYCLE_1) == (1008, 850)


def test_cpu_percent_between_two_samples() -> None:
    prev = _CpuSample(at=0.0, total=1008, idle=850)
    curr = _CpuSample(at=1.0, total=1073, idle=905)
    pct = _cpu_percent(prev, curr)
    assert pct is not None
    assert round(pct, 2) == 15.38


def test_cpu_percent_none_when_total_did_not_advance() -> None:
    sample = _CpuSample(at=0.0, total=1008, idle=850)
    assert _cpu_percent(sample, sample) is None


def test_parse_meminfo() -> None:
    result = _parse_meminfo(MEMINFO_TEXT)
    assert result is not None
    total, available = result
    assert total == 16384000 * 1024
    assert available == 8000000 * 1024


def test_parse_net_dev_excludes_loopback() -> None:
    result = _parse_net_dev(NETDEV_TEXT)
    assert result is not None
    rx, tx = result
    assert (rx, tx) == (5000000, 3000000)


def test_parse_diskstats_sums_whole_devices_only() -> None:
    result = _parse_diskstats(DISKSTATS_TEXT)
    assert result is not None
    read_bytes, write_bytes = result
    # sda: rd_sectors=20000, wr_sectors=40000; nvme0n1: 60000 / 80000.
    # sda1, nvme0n1p1 (partitions) and loop0 (virtual) are excluded.
    assert read_bytes == (20000 + 60000) * 512
    assert write_bytes == (40000 + 80000) * 512


def test_split_proc_sections() -> None:
    combined = "\n".join(
        [
            ss._MARKER_STAT,
            "cpu  1 2 3 4",
            ss._MARKER_MEMINFO,
            "MemTotal: 100 kB",
            ss._MARKER_NETDEV,
            "  eth0: 1 2 3 4 5 6 7 8 9",
            ss._MARKER_DISKSTATS,
            "8 0 sda 1 2 3 4 5 6 7 8",
        ]
    )
    sections = _split_proc_sections(combined)
    assert sections[ss._MARKER_STAT] == "cpu  1 2 3 4"
    assert sections[ss._MARKER_MEMINFO] == "MemTotal: 100 kB"


async def test_try_sample_docker_disk_usage_normalizes_sizes() -> None:
    rows = [
        {
            "Type": "Images",
            "TotalCount": 10,
            "Active": 3,
            "Size": "1.5GB",
            "Reclaimable": "500MB (33%)",
        },
        {
            "Type": "Containers",
            "TotalCount": 5,
            "Active": 2,
            "Size": "200MB",
            "Reclaimable": "0B (0%)",
        },
    ]
    transport = FakeTransport(
        run_result=ExecResult(
            returncode=0, stdout="\n".join(json.dumps(r) for r in rows), stderr=""
        )
    )

    usage = await _try_sample_docker_disk_usage("daemon-a", transport)

    assert usage is not None
    images = usage["Images"]
    assert isinstance(images, dict)
    assert images["size_bytes"] == 1_500_000_000
    assert images["reclaimable_bytes"] == 500_000_000


async def test_try_sample_docker_disk_usage_returns_none_on_transport_error() -> None:
    transport = FakeTransport(raise_on_start=TransportError("unreachable"))
    assert await _try_sample_docker_disk_usage("daemon-a", transport) is None


async def test_try_sample_container_aggregate_sums_across_containers() -> None:
    rows = [
        {"CPUPerc": "10.0%", "MemUsage": "100MiB / 1GiB", "NetIO": "1kB / 2kB"},
        {"CPUPerc": "5.0%", "MemUsage": "50MiB / 1GiB", "NetIO": "3kB / 4kB"},
    ]
    transport = FakeTransport(
        run_result=ExecResult(
            returncode=0, stdout="\n".join(json.dumps(r) for r in rows), stderr=""
        )
    )

    metrics = await _try_sample_container_aggregate("daemon-a", transport)

    assert metrics["cpu_pct"] == 15.0
    assert metrics["mem_used_bytes"] == 150 * 1024 * 1024
    assert metrics["net_rx_bytes"] == 4000
    assert metrics["net_tx_bytes"] == 6000


async def test_run_system_stats_with_source_none_only_emits_docker_disk_usage() -> None:
    df_row = {
        "Type": "Images",
        "TotalCount": 1,
        "Active": 1,
        "Size": "1GB",
        "Reclaimable": "0B (0%)",
    }
    transport = FakeTransport(
        run_result=ExecResult(returncode=0, stdout=json.dumps(df_row), stderr="")
    )
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_system_stats(
            "daemon-a",
            transport,
            records_logger,
            stats_interval_s=10.0,
            system_metrics_source="none",
        )
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
    assert record.container_id == SYSTEM_SCOPE_ID
    assert record.container_name == SYSTEM_SCOPE_ID
    assert record.metric_scope == "system"
    assert record.source == "docker system df"
    assert record.cpu_pct is None
    assert record.system is not None
    assert "docker_disk_usage" in record.system


async def test_run_system_stats_falls_back_to_container_aggregate_when_proc_fails() -> None:
    transport = FakeTransport(raise_on_start=TransportError("unreachable"))
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_system_stats(
            "daemon-a",
            transport,
            records_logger,
            stats_interval_s=10.0,
            system_metrics_source="proc",
        )
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
    # docker system df also fails (same transport), so no docker_disk_usage,
    # and the container-aggregate attempt also fails -- but the loop must
    # still emit a record rather than getting stuck, per spec's "never fatal".
    assert record.source == "container-aggregate"
