"""services-listing (swarm support, migration plan Phase 1b): one task per
daemon.

Periodically runs the equivalent of `docker service ls` and ships one
`ServiceRecord` per currently-listed swarm service through the exact same
`records_logger` -> Logstash -> Redis pipeline every log/metric record
already uses (see `logging_setup.py`) -- no direct Redis access from
log-listener, matching every other listener task (see
`containers_listing.py`'s own docstring on why `on_cycle` exists instead of
a direct write).

Deliberately not wired into `ListenerManager`/`ContainerTracker` the way
containers are: there is no per-service task to spawn/stop. A service's own
task containers already show up in ordinary `docker ps`/`docker stats` and
get collected as ordinary containers -- `queries.py`'s query-time grouping
by service name (`_metric_group`) is what actually merges them into one
service timeline. This task exists purely so a client can discover *which*
services currently exist (cttc's `docker_ps`'s "services" list, used by the
Set Sources picker to offer a whole service as a collection target), not to
collect anything itself.

Tolerates "not a swarm manager" the same way cttc's own `docker_ps` does: a
failed cycle just means nothing ships this round, not a fatal error, so a
non-swarm daemon (the common case) never spams a warning every cycle.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from datetime import UTC, datetime

import structlog

from log_sump.common.schema import RecordAdapter, ServiceRecord
from log_sump.common.transport import Transport, TransportError

from .logging_setup import RecordsLogger

logger = structlog.get_logger(__name__)


async def run_services_listing(
    docker_host: str,
    transport: Transport,
    records_logger: RecordsLogger,
    *,
    listing_interval_s: float,
) -> None:
    seq_counter = itertools.count(1)
    while True:
        try:
            await _sample_once(docker_host, transport, records_logger, seq_counter)
        except (TransportError, ValueError) as exc:
            # Not a swarm manager (the common case) or a transient failure --
            # same tolerance cttc's own docker_ps's `docker service ls` try/
            # except has. Debug, not warning: this fires every cycle on every
            # non-swarm daemon, so anything louder would be pure noise.
            await logger.adebug(
                "services_listing.cycle_failed", docker_host=docker_host, error=str(exc)
            )
        await asyncio.sleep(listing_interval_s)


async def _sample_once(
    docker_host: str,
    transport: Transport,
    records_logger: RecordsLogger,
    seq_counter: itertools.count[int],
) -> None:
    result = await transport.run(["docker", "service", "ls", "--format", "{{json .}}"])
    result.check()

    now = datetime.now(UTC)
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        record = ServiceRecord(
            docker_host=docker_host,
            ts=now,
            seq=next(seq_counter),
            id=data["ID"][:12],
            name=data["Name"],
            replicas=str(data.get("Replicas", "")),
        )
        await records_logger.ainfo(RecordAdapter.dump_json(record).decode())
