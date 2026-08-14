"""container-stats (spec §5.1): one task per daemon.

Polls `docker stats --no-stream --format '{{json .}}'` once per cycle — a
single snapshot covering every running container on the daemon, not one
subprocess per container — normalizes the human-readable units to numeric
values (spec §6), and emits one `kind = "metric"` record per container.
Only samples containers currently known to the registry, so a container
that has already been torn down doesn't get one last stray sample.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from collections.abc import Mapping
from datetime import UTC, datetime

import structlog
from log_sump_common.schema import MetricRecord, RecordAdapter
from log_sump_common.transport import Transport, TransportError

from .logging_setup import RecordsLogger
from .registry import Registry
from .units import parse_pair_bytes, parse_percent

logger = structlog.get_logger(__name__)


async def run_container_stats(
    docker_host: str,
    transport: Transport,
    registry: Registry,
    records_logger: RecordsLogger,
    *,
    stats_interval_s: float,
) -> None:
    seq_counter = itertools.count(1)
    while True:
        try:
            await _sample_once(docker_host, transport, registry, records_logger, seq_counter)
        except (TransportError, ValueError) as exc:
            await logger.awarning(
                "container_stats.cycle_failed", docker_host=docker_host, error=str(exc)
            )
        await asyncio.sleep(stats_interval_s)


async def _sample_once(
    docker_host: str,
    transport: Transport,
    registry: Registry,
    records_logger: RecordsLogger,
    seq_counter: itertools.count[int],
) -> None:
    result = await transport.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"])
    result.check()

    now = datetime.now(UTC)
    known_ids = set(registry.state_for(docker_host).containers)
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        if data.get("ID") not in known_ids:
            continue  # already torn down since the last containers-listing cycle
        record = _parse_stats_line(
            docker_host=docker_host, data=data, seq=next(seq_counter), ts=now
        )
        await records_logger.ainfo(RecordAdapter.dump_json(record).decode())


def _parse_stats_line(
    *, docker_host: str, data: Mapping[str, object], seq: int, ts: datetime
) -> MetricRecord:
    mem_used, mem_limit = parse_pair_bytes(str(data.get("MemUsage", "")))
    net_rx, net_tx = parse_pair_bytes(str(data.get("NetIO", "")))
    blk_read, blk_write = parse_pair_bytes(str(data.get("BlockIO", "")))

    return MetricRecord(
        docker_host=docker_host,
        container_name=str(data.get("Name", "")),
        container_id=str(data.get("ID", "")),
        ts=ts,
        seq=seq,
        metric_scope="container",
        cpu_pct=parse_percent(str(data.get("CPUPerc", ""))),
        mem_used_bytes=mem_used,
        mem_limit_bytes=mem_limit,
        mem_pct=parse_percent(str(data.get("MemPerc", ""))),
        net_rx_bytes=net_rx,
        net_tx_bytes=net_tx,
        blk_read_bytes=blk_read,
        blk_write_bytes=blk_write,
        pids=_parse_int(data.get("PIDs")),
        source="docker stats",
        raw=data,
    )


def _parse_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None
