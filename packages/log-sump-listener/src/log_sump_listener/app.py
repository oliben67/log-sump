"""Orchestrator: wires transports, the registry, containers-listing,
container-tracker, and per-container listeners together for every enabled
daemon (spec §5.1, §9).

The bounded-concurrency semaphore around spawning caps how many listener
startups can be in flight at once, process-wide, so a burst of new
containers on any daemon can't starve the event loop (spec §5.1 "Async
requirements").

`Registry` supports exactly one `on_new_container` callback (see
registry.py), so `run()` owns a single dispatcher keyed by `docker_host` and
routes to that daemon's own `ListenerManager` — registering the callback
inside `run_daemon` instead would let the last-started daemon silently
overwrite every other daemon's spawn wiring.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import structlog
from log_sump_common.config import DaemonConfig, Settings
from log_sump_common.transport import LocalTransport, SSHTransport, Transport

from .container_listener import run_container_listener
from .container_stats import run_container_stats
from .container_tracker import ContainerTracker
from .containers_listing import run_containers_listing
from .logging_setup import RecordsLogger
from .registry import ContainerRef, Registry
from .system_stats import run_system_stats

logger = structlog.get_logger(__name__)


def build_transport(daemon: DaemonConfig) -> Transport:
    if daemon.transport == "local":
        return LocalTransport()
    return SSHTransport(host=daemon.host, user=daemon.user, ssh_options=daemon.ssh_options)


class ListenerManager:
    """Owns the real per-container `container_listener` asyncio.Tasks for one daemon."""

    def __init__(
        self,
        docker_host: str,
        transport: Transport,
        records_logger: RecordsLogger,
        spawn_semaphore: asyncio.Semaphore,
    ) -> None:
        self._docker_host = docker_host
        self._transport = transport
        self._records_logger = records_logger
        self._spawn_semaphore = spawn_semaphore
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def active_container_ids(self) -> set[str]:
        return set(self._tasks)

    async def spawn(self, ref: ContainerRef) -> None:
        if ref.container_id in self._tasks:
            return
        async with self._spawn_semaphore:
            if ref.container_id in self._tasks:  # re-check: lost the race while waiting
                return
            task = asyncio.create_task(
                run_container_listener(
                    self._docker_host,
                    ref.container_id,
                    ref.container_name,
                    self._transport,
                    self._records_logger,
                ),
                name=f"container-listener:{self._docker_host}:{ref.container_id}",
            )
            self._tasks[ref.container_id] = task
            task.add_done_callback(self._make_done_callback(ref.container_id))
            await logger.ainfo(
                "listener.spawned",
                docker_host=self._docker_host,
                container_id=ref.container_id,
                container_name=ref.container_name,
            )

    def _make_done_callback(self, container_id: str) -> Callable[[asyncio.Task[None]], None]:
        def on_done(task: asyncio.Task[None]) -> None:
            # The listener can end on its own (container exited, transport
            # error) as well as via stop() below -- either way, drop our
            # bookkeeping so a future registry re-discovery can respawn it.
            if self._tasks.get(container_id) is task:
                del self._tasks[container_id]
            if not task.cancelled() and (exc := task.exception()) is not None:
                # Synchronous logging call: add_done_callback runs on the
                # loop, not in a coroutine, so there's no `await` here to
                # offload with. python-logstash-async's handler enqueues
                # onto its own worker thread regardless of sync/async call,
                # so this still doesn't block on I/O.
                logger.error(
                    "listener.crashed",
                    docker_host=self._docker_host,
                    container_id=container_id,
                    error=str(exc),
                )

        return on_done

    async def stop(self, container_id: str) -> None:
        task = self._tasks.pop(container_id, None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def run_daemon(
    daemon: DaemonConfig,
    settings: Settings,
    registry: Registry,
    transport: Transport,
    listeners: ListenerManager,
    records_logger: RecordsLogger,
) -> None:
    tracker = ContainerTracker(
        registry,
        daemon.id,
        active_container_ids=listeners.active_container_ids,
        stop_listener=listeners.stop,
        tracker_interval_s=settings.listener.tracker_interval_s,
        missing_threshold_cycles=settings.listener.missing_threshold_cycles,
    )
    tasks = [
        run_containers_listing(
            daemon.id,
            transport,
            registry,
            listing_interval_s=settings.listener.listing_interval_s,
        ),
        tracker.run_forever(),
    ]
    if settings.metrics.enabled:
        tasks.append(
            run_container_stats(
                daemon.id,
                transport,
                registry,
                records_logger,
                stats_interval_s=settings.listener.stats_interval_s,
            )
        )
        if settings.metrics.system_enabled:
            tasks.append(
                run_system_stats(
                    daemon.id,
                    transport,
                    records_logger,
                    stats_interval_s=settings.listener.effective_system_stats_interval_s(),
                    system_metrics_source=settings.metrics.system_metrics_source,
                )
            )
    await asyncio.gather(*tasks)


def _build_new_container_dispatcher(
    listeners_by_daemon: dict[str, ListenerManager],
) -> Callable[[str, ContainerRef], Awaitable[None]]:
    """One dispatcher shared by every daemon, routing by `docker_host`.

    `Registry` only holds a single callback slot (see registry.py), so this
    must be the *only* place `registry.on_new_container` is called — wiring
    it separately per daemon would let the last daemon silently clobber
    every other daemon's spawn routing.
    """

    async def on_new_container(docker_host: str, ref: ContainerRef) -> None:
        await listeners_by_daemon[docker_host].spawn(ref)

    return on_new_container


async def run(settings: Settings, records_logger: RecordsLogger) -> None:
    enabled = settings.enabled_daemons()
    if not enabled:
        await logger.awarning("app.no_enabled_daemons")
        return

    registry = Registry()
    spawn_semaphore = asyncio.Semaphore(settings.listener.max_concurrent_listener_spawns)
    transports_by_daemon = {daemon.id: build_transport(daemon) for daemon in enabled}
    listeners_by_daemon = {
        daemon.id: ListenerManager(
            daemon.id, transports_by_daemon[daemon.id], records_logger, spawn_semaphore
        )
        for daemon in enabled
    }
    registry.on_new_container(_build_new_container_dispatcher(listeners_by_daemon))

    await logger.ainfo("app.starting", daemon_ids=list(listeners_by_daemon))
    await asyncio.gather(
        *(
            run_daemon(
                daemon,
                settings,
                registry,
                transports_by_daemon[daemon.id],
                listeners_by_daemon[daemon.id],
                records_logger,
            )
            for daemon in enabled
        )
    )
