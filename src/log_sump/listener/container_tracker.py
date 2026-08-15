"""container-tracker (spec §5.1): one instance per daemon.

Reconciles active listeners against the registry and stops the ones whose
containers have vanished from `docker ps` for `missing_threshold_cycles`
consecutive listings. A container's miss count is `listing_seq -
last_seen_seq`; `Registry.update` resets `last_seen_seq` to the current
`listing_seq` every time the container reappears, so "reset the miss count
on reappearance" (spec) falls out of that arithmetic for free — this class
doesn't need to track misses itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable

import structlog

from .registry import Registry

logger = structlog.get_logger(__name__)

StopListener = Callable[[str], Awaitable[None]]
ActiveContainerIds = Callable[[], Iterable[str]]


class ContainerTracker:
    def __init__(
        self,
        registry: Registry,
        docker_host: str,
        *,
        active_container_ids: ActiveContainerIds,
        stop_listener: StopListener,
        tracker_interval_s: float,
        missing_threshold_cycles: int,
    ) -> None:
        self._registry = registry
        self._docker_host = docker_host
        self._active_container_ids = active_container_ids
        self._stop_listener = stop_listener
        self._tracker_interval_s = tracker_interval_s
        self._missing_threshold_cycles = missing_threshold_cycles

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(self._tracker_interval_s)
            await self.run_once()

    async def run_once(self) -> None:
        state = self._registry.state_for(self._docker_host)
        for container_id in list(self._active_container_ids()):
            container_state = state.containers.get(container_id)
            if container_state is None:
                # Not in the registry at all (already forgotten, or never
                # valid) — nothing left to compare against; stop it.
                misses = self._missing_threshold_cycles
            else:
                misses = state.listing_seq - container_state.last_seen_seq

            if misses >= self._missing_threshold_cycles:
                await logger.ainfo(
                    "container_tracker.stopping_listener",
                    docker_host=self._docker_host,
                    container_id=container_id,
                    misses=misses,
                )
                await self._stop_listener(container_id)
                self._registry.forget(self._docker_host, container_id)
