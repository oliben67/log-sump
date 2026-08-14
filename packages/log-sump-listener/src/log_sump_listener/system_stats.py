"""system-stats (spec §5.1): one task per daemon.

Emits daemon/host-level metric records (`container_id`/`container_name` =
`"__system__"`, spec §6): Docker disk usage via `docker system df` (always
attempted, regardless of `system_metrics_source`), plus host CPU/memory/
network/disk from `/proc` when `system_metrics_source` is `"proc"` (spec
§11.6's default source) — read over the transport in a single round trip via
`Transport.run_shell`, since four separate SSH round trips per daemon per
cycle would add real, avoidable latency for remote hosts.

`net_rx_bytes`/`net_tx_bytes`/`blk_read_bytes`/`blk_write_bytes` are reported
as **cumulative counters** (bytes since boot), matching `container_stats.py`
and `docker stats`' own `NetIO`/`BlockIO` semantics — not rates. `cpu_pct` is
the one field that's inherently a rate (there's no cumulative form of "CPU
percent"), so it's the only figure computed from a delta against the
previous cycle's `/proc/stat` sample; the first cycle after (re)start has no
prior sample to diff against, so `cpu_pct` is `None` until the second cycle.

If `/proc` reading fails, or `system_metrics_source` is `"container-aggregate"`,
falls back to a coarse proxy: aggregating that cycle's `docker stats` across
all containers (§11.6: "fall back to an aggregate of all containers' docker
stats... and flag it as such"). `"none"` skips host-level CPU/memory/network/
disk entirely, emitting only Docker disk usage. Every failure is caught,
logged, and skipped — this loop must never crash (spec §5.1's "Host-level
collection is best-effort").
"""

from __future__ import annotations

import asyncio
import itertools
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from log_sump_common.schema import SYSTEM_SCOPE_ID, MetricRecord, RecordAdapter
from log_sump_common.transport import Transport, TransportError

from .logging_setup import RecordsLogger
from .units import parse_bytes, parse_pair_bytes, parse_percent

logger = structlog.get_logger(__name__)

_MARKER_STAT = "===STAT==="
_MARKER_MEMINFO = "===MEMINFO==="
_MARKER_NETDEV = "===NETDEV==="
_MARKER_DISKSTATS = "===DISKSTATS==="

_PROC_READ_SCRIPT = (
    f"echo {_MARKER_STAT}; cat /proc/stat; "
    f"echo {_MARKER_MEMINFO}; cat /proc/meminfo; "
    f"echo {_MARKER_NETDEV}; cat /proc/net/dev; "
    f"echo {_MARKER_DISKSTATS}; cat /proc/diskstats"
)

# Common partition-naming schemes to exclude when summing /proc/diskstats,
# so whole-device and partition entries aren't double-counted. Best-effort:
# unusual device-naming schemes may still be under/over-counted.
_PARTITION_RE = re.compile(r"^(?:sd[a-z]+|hd[a-z]+|vd[a-z]+|xvd[a-z]+)\d+$|^nvme\d+n\d+p\d+$")
_VIRTUAL_DEVICE_PREFIXES = ("loop", "ram", "dm-")


@dataclass(frozen=True)
class _CpuSample:
    at: float
    total: int
    idle: int


async def run_system_stats(
    docker_host: str,
    transport: Transport,
    records_logger: RecordsLogger,
    *,
    stats_interval_s: float,
    system_metrics_source: str,
) -> None:
    seq_counter = itertools.count(1)
    prev_cpu: _CpuSample | None = None
    while True:
        docker_disk = await _try_sample_docker_disk_usage(docker_host, transport)

        host_metrics: dict[str, float | int | None] = {}
        source = "docker system df"
        if system_metrics_source == "proc":
            sampled = await _try_sample_proc(docker_host, transport)
            if sampled is not None:
                host_metrics, cpu_sample = sampled
                if prev_cpu is not None:
                    host_metrics["cpu_pct"] = _cpu_percent(prev_cpu, cpu_sample)
                prev_cpu = cpu_sample
                source = "/proc"
            else:
                host_metrics = await _try_sample_container_aggregate(docker_host, transport)
                source = "container-aggregate"
        elif system_metrics_source == "container-aggregate":
            host_metrics = await _try_sample_container_aggregate(docker_host, transport)
            source = "container-aggregate"
        # "none": host_metrics stays empty; source stays "docker system df"

        record = MetricRecord(
            docker_host=docker_host,
            container_name=SYSTEM_SCOPE_ID,
            container_id=SYSTEM_SCOPE_ID,
            ts=datetime.now(UTC),
            seq=next(seq_counter),
            metric_scope="system",
            cpu_pct=host_metrics.get("cpu_pct"),
            mem_used_bytes=host_metrics.get("mem_used_bytes"),
            mem_limit_bytes=host_metrics.get("mem_limit_bytes"),
            mem_pct=host_metrics.get("mem_pct"),
            net_rx_bytes=host_metrics.get("net_rx_bytes"),
            net_tx_bytes=host_metrics.get("net_tx_bytes"),
            blk_read_bytes=host_metrics.get("blk_read_bytes"),
            blk_write_bytes=host_metrics.get("blk_write_bytes"),
            system={"docker_disk_usage": docker_disk} if docker_disk is not None else None,
            source=source,
            raw=None,
        )
        await records_logger.ainfo(RecordAdapter.dump_json(record).decode())
        await asyncio.sleep(stats_interval_s)


# ---------------------------------------------------------------------------
# docker system df


async def _try_sample_docker_disk_usage(
    docker_host: str, transport: Transport
) -> dict[str, object] | None:
    try:
        result = await transport.run(["docker", "system", "df", "--format", "{{json .}}"])
        result.check()
    except (TransportError, ValueError) as exc:
        await logger.awarning(
            "system_stats.docker_df_failed", docker_host=docker_host, error=str(exc)
        )
        return None

    rows: dict[str, object] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        row_type = data.get("Type")
        if not row_type:
            continue
        rows[row_type] = {
            "total": data.get("TotalCount"),
            "active": data.get("Active"),
            "size_bytes": parse_bytes(str(data.get("Size", ""))),
            "reclaimable_bytes": _parse_reclaimable(str(data.get("Reclaimable", ""))),
            "raw": data,
        }
    return rows or None


def _parse_reclaimable(text: str) -> int | None:
    # e.g. "500MB (80%)" -- strip the trailing percentage before parsing.
    return parse_bytes(text.split("(")[0].strip())


# ---------------------------------------------------------------------------
# /proc


async def _try_sample_proc(
    docker_host: str, transport: Transport
) -> tuple[dict[str, float | int | None], _CpuSample] | None:
    try:
        result = await transport.run_shell(_PROC_READ_SCRIPT)
        result.check()
        sections = _split_proc_sections(result.stdout)
        cpu = _parse_cpu_total_idle(sections.get(_MARKER_STAT, ""))
        mem = _parse_meminfo(sections.get(_MARKER_MEMINFO, ""))
        if cpu is None or mem is None:
            raise ValueError("could not parse /proc/stat or /proc/meminfo")
    except (TransportError, ValueError) as exc:
        await logger.awarning(
            "system_stats.proc_sample_failed", docker_host=docker_host, error=str(exc)
        )
        return None

    cpu_total, cpu_idle = cpu
    mem_total, mem_available = mem
    net = _parse_net_dev(sections.get(_MARKER_NETDEV, ""))
    disk = _parse_diskstats(sections.get(_MARKER_DISKSTATS, ""))

    metrics: dict[str, float | int | None] = {
        "mem_used_bytes": mem_total - mem_available,
        "mem_limit_bytes": mem_total,
        "mem_pct": (100.0 * (mem_total - mem_available) / mem_total) if mem_total else None,
    }
    if net is not None:
        metrics["net_rx_bytes"], metrics["net_tx_bytes"] = net
    if disk is not None:
        metrics["blk_read_bytes"], metrics["blk_write_bytes"] = disk

    return metrics, _CpuSample(at=time.monotonic(), total=cpu_total, idle=cpu_idle)


def _split_proc_sections(stdout: str) -> dict[str, str]:
    markers = {_MARKER_STAT, _MARKER_MEMINFO, _MARKER_NETDEV, _MARKER_DISKSTATS}
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for line in stdout.splitlines():
        if line in markers:
            current = line
            sections[current] = []
            continue
        if current is not None:
            sections[current].append(line)
    return {marker: "\n".join(lines) for marker, lines in sections.items()}


def _parse_cpu_total_idle(stat_text: str) -> tuple[int, int] | None:
    for line in stat_text.splitlines():
        if line.startswith("cpu "):
            values = [int(p) for p in line.split()[1:] if p.isdigit()]
            if len(values) < 4:
                return None
            idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
            return sum(values), idle
    return None


def _parse_meminfo(meminfo_text: str) -> tuple[int, int] | None:
    """Returns `(mem_total_bytes, mem_available_bytes)`."""
    values: dict[str, int] = {}
    for line in meminfo_text.splitlines():
        key, _, rest = line.partition(":")
        rest = rest.strip()
        if rest.endswith("kB"):
            try:
                values[key] = int(rest[:-2].strip()) * 1024
            except ValueError:
                continue
    if "MemTotal" not in values:
        return None
    total = values["MemTotal"]
    return total, values.get("MemAvailable", total)


def _parse_net_dev(netdev_text: str) -> tuple[int, int] | None:
    """Sums cumulative RX/TX bytes across all interfaces except loopback."""
    rx_total = 0
    tx_total = 0
    found = False
    for line in netdev_text.splitlines():
        iface, sep, rest = line.partition(":")
        iface = iface.strip()
        if not sep or iface == "lo" or not iface:
            continue
        fields = rest.split()
        if len(fields) < 9:
            continue
        try:
            rx_total += int(fields[0])
            tx_total += int(fields[8])
        except ValueError:
            continue
        found = True
    return (rx_total, tx_total) if found else None


def _parse_diskstats(diskstats_text: str) -> tuple[int, int] | None:
    """Sums cumulative read/write bytes (sectors * 512) across whole-device entries."""
    read_sectors = 0
    write_sectors = 0
    found = False
    for line in diskstats_text.splitlines():
        fields = line.split()
        if len(fields) < 10:
            continue
        name = fields[2]
        if name.startswith(_VIRTUAL_DEVICE_PREFIXES) or _PARTITION_RE.match(name):
            continue
        try:
            read_sectors += int(fields[5])
            write_sectors += int(fields[9])
        except ValueError:
            continue
        found = True
    if not found:
        return None
    return read_sectors * 512, write_sectors * 512


def _cpu_percent(prev: _CpuSample, curr: _CpuSample) -> float | None:
    d_total = curr.total - prev.total
    d_idle = curr.idle - prev.idle
    if d_total <= 0:
        return None
    return max(0.0, 100.0 * (1 - d_idle / d_total))


# ---------------------------------------------------------------------------
# container-aggregate fallback


async def _try_sample_container_aggregate(
    docker_host: str, transport: Transport
) -> dict[str, float | int | None]:
    try:
        result = await transport.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"])
        result.check()
    except (TransportError, ValueError) as exc:
        await logger.awarning(
            "system_stats.container_aggregate_failed", docker_host=docker_host, error=str(exc)
        )
        return {}

    cpu_pct_total = 0.0
    mem_used_total = 0
    net_rx_total = 0
    net_tx_total = 0
    count = 0
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        cpu_pct_total += parse_percent(str(data.get("CPUPerc", ""))) or 0.0
        mem_used, _mem_limit = parse_pair_bytes(str(data.get("MemUsage", "")))
        net_rx, net_tx = parse_pair_bytes(str(data.get("NetIO", "")))
        mem_used_total += mem_used or 0
        net_rx_total += net_rx or 0
        net_tx_total += net_tx or 0
        count += 1

    if count == 0:
        return {}
    return {
        "cpu_pct": cpu_pct_total,
        "mem_used_bytes": mem_used_total,
        "net_rx_bytes": net_rx_total,
        "net_tx_bytes": net_tx_total,
    }
