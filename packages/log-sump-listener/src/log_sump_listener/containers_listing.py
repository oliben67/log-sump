"""containers-listing (spec §5.1): one task per daemon.

Periodically runs the equivalent of `docker ps`, builds the current running
container set, and hands it to the central registry. Daemon-reachability
failures are caught, logged, and retried next cycle — this loop must never
crash, or the daemon silently stops being watched.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import structlog
from log_sump_common.transport import Transport, TransportError

from .registry import ContainerRef, Registry

logger = structlog.get_logger(__name__)

#: Called once per cycle with `(reachable, container_count)` — the seam a
#: future daemon-reachability status publisher (spec §10) hooks into,
#: without `containers_listing` itself needing to know about Redis.
CycleCallback = Callable[[bool, int], Awaitable[None]]


async def run_containers_listing(
    docker_host: str,
    transport: Transport,
    registry: Registry,
    *,
    listing_interval_s: float,
    on_cycle: CycleCallback | None = None,
) -> None:
    while True:
        try:
            listing = await _list_containers(transport)
        except (TransportError, ValueError) as exc:
            await logger.awarning(
                "containers_listing.cycle_failed", docker_host=docker_host, error=str(exc)
            )
            if on_cycle is not None:
                await on_cycle(False, 0)
        else:
            await registry.update(docker_host, listing)
            if on_cycle is not None:
                await on_cycle(True, len(listing))
        await asyncio.sleep(listing_interval_s)


async def _list_containers(transport: Transport) -> set[ContainerRef]:
    result = await transport.run(["docker", "ps", "--format", "{{json .}}"])
    result.check()

    refs: set[ContainerRef] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        refs.add(ContainerRef(container_id=data["ID"], container_name=data["Names"]))
    return refs
