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
from redis.asyncio import Redis
from redis.exceptions import RedisError

from log_sump.common.config import DaemonConfig, Settings
from log_sump.common.daemon_registry import list_registered_daemons
from log_sump.common.transport import LocalTransport, SSHTransport, Transport

from .container_listener import run_container_listener
from .container_stats import run_container_stats
from .container_tracker import ContainerTracker
from .containers_listing import run_containers_listing
from .logging_setup import RecordsLogger
from .registry import ContainerRef, Registry
from .services_listing import run_services_listing
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
        # Discovery only (cttc's docker_ps's "services" list) -- always on,
        # like containers-listing, and just as tolerant of a non-swarm
        # daemon (the common case: every cycle no-ops, see its own
        # docstring). Not gated by settings.metrics.enabled -- it ships no
        # metrics of its own, just which services currently exist.
        run_services_listing(
            daemon.id,
            transport,
            records_logger,
            listing_interval_s=settings.listener.listing_interval_s,
        ),
    ]
    if settings.metrics.enabled:
        tasks.append(
            run_container_stats(
                daemon.id,
                transport,
                registry,
                records_logger,
                stats_interval_s=settings.listener.stats_interval_s,
                watched_containers=daemon.watched_containers,
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
                    # local_proc_root only ever applies to transport: local
                    # -- an SSH-reached daemon's /proc read runs on *that*
                    # remote machine, always at the literal /proc.
                    proc_root=settings.listener.local_proc_root
                    if daemon.transport == "local"
                    else "/proc",
                )
            )
    await asyncio.gather(*tasks)


def _is_watched(container_name: str, watched_containers: list[str] | None) -> bool:
    return watched_containers is None or container_name in watched_containers


def _build_new_container_dispatcher(
    listeners_by_daemon: dict[str, ListenerManager],
    daemon_configs: dict[str, DaemonConfig],
) -> Callable[[str, ContainerRef], Awaitable[None]]:
    """One dispatcher shared by every daemon, routing by `docker_host`.

    `Registry` only holds a single callback slot (see registry.py), so this
    must be the *only* place `registry.on_new_container` is called — wiring
    it separately per daemon would let the last daemon silently clobber
    every other daemon's spawn routing.

    `daemon_configs` is read live (looked up by key at call time, same as
    `listeners_by_daemon`), not captured once -- `watched_containers`
    (selective collection, migration plan Phase 9) can change after a
    daemon's already running (`PATCH /daemons/{id}`), and the registry
    keeps discovering *every* container on the daemon regardless (registry.py
    itself has no filter -- only this dispatch point decides whether a
    discovered container actually gets a listener spawned).
    """

    async def on_new_container(docker_host: str, ref: ContainerRef) -> None:
        daemon = daemon_configs.get(docker_host)
        watched = daemon.watched_containers if daemon is not None else None
        if not _is_watched(ref.container_name, watched):
            return
        await listeners_by_daemon[docker_host].spawn(ref)

    return on_new_container


class DaemonManager:
    """Owns the real per-daemon `run_daemon` asyncio.Tasks (migration plan
    Phase 3) -- the exact `ListenerManager.spawn`/`stop` pattern above,
    applied one level up: a daemon can now be added/removed at runtime the
    same way a container already could. `listeners_by_daemon` grows as
    daemons are spawned; `_build_new_container_dispatcher` already looks
    its target up by key at call time (not once at construction), so
    reusing it here needs no changes for that dynamic growth to work.
    """

    def __init__(self, settings: Settings, records_logger: RecordsLogger) -> None:
        self._settings = settings
        self._records_logger = records_logger
        self._registry = Registry()
        self._spawn_semaphore = asyncio.Semaphore(settings.listener.max_concurrent_listener_spawns)
        self._listeners_by_daemon: dict[str, ListenerManager] = {}
        self._daemon_configs: dict[str, DaemonConfig] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._registry.on_new_container(
            _build_new_container_dispatcher(self._listeners_by_daemon, self._daemon_configs)
        )

    def active_daemon_ids(self) -> set[str]:
        return set(self._tasks)

    def current_config(self, daemon_id: str) -> DaemonConfig | None:
        """The `DaemonConfig` a currently-running daemon was last spawned
        (or respawned) with -- lets a caller (`run_daemon_registry_watch`)
        tell whether the registry's own copy has changed since, e.g. a
        `PATCH /daemons/{id}` updating `watched_containers`.
        """
        return self._daemon_configs.get(daemon_id)

    async def spawn(self, daemon: DaemonConfig) -> None:
        if daemon.id in self._tasks:
            return
        transport = build_transport(daemon)
        listeners = ListenerManager(
            daemon.id, transport, self._records_logger, self._spawn_semaphore
        )
        self._listeners_by_daemon[daemon.id] = listeners
        self._daemon_configs[daemon.id] = daemon
        coro = run_daemon(
            daemon, self._settings, self._registry, transport, listeners, self._records_logger
        )
        task = asyncio.create_task(coro, name=f"daemon:{daemon.id}")
        self._tasks[daemon.id] = task
        task.add_done_callback(self._make_done_callback(daemon.id))
        await logger.ainfo("daemon_manager.spawned", docker_host=daemon.id)

    def _make_done_callback(self, daemon_id: str) -> Callable[[asyncio.Task[None]], None]:
        def on_done(task: asyncio.Task[None]) -> None:
            # A daemon's whole task tree can end on its own (every one of
            # its subtasks failing) as well as via stop() below -- either
            # way, drop our bookkeeping so a future registry re-add can
            # respawn it, matching ListenerManager's own done-callback.
            if self._tasks.get(daemon_id) is task:
                del self._tasks[daemon_id]
            self._listeners_by_daemon.pop(daemon_id, None)
            self._daemon_configs.pop(daemon_id, None)
            if not task.cancelled() and (exc := task.exception()) is not None:
                logger.error("daemon_manager.crashed", docker_host=daemon_id, error=str(exc))

        return on_done

    async def stop(self, daemon_id: str) -> None:
        task = self._tasks.pop(daemon_id, None)
        self._listeners_by_daemon.pop(daemon_id, None)
        self._daemon_configs.pop(daemon_id, None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def run_daemon_registry_watch(
    redis: Redis,
    manager: DaemonManager,
    yaml_daemon_ids: frozenset[str],
    *,
    poll_interval_s: float,
) -> None:
    """Polls the runtime daemon registry (migration plan Phase 3,
    `daemon_registry.py`) and reconciles it against `manager`'s currently
    running daemons: spawns anything newly registered, stops anything
    that's been removed, and *respawns* anything whose registered config no
    longer matches what it was last spawned with -- e.g. `watched_containers`
    (migration plan Phase 9's selective collection) updated via `PATCH
    /daemons/{id}` after the daemon was already running. A full stop+spawn,
    not a live in-place filter update: it reuses the exact machinery an
    ordinary add/remove already exercises, at the cost of a brief collection
    gap even for containers whose watched status didn't change -- log-sump's
    Streams aren't affected either way, so nothing is lost, just delayed a
    cycle. Never touches a daemon in `yaml_daemon_ids` -- those are config's
    own responsibility (a restart, not this loop, is what removes or
    changes one), even if it's absent from the registry (it was never
    supposed to be there in the first place).
    """
    while True:
        try:
            registered = await list_registered_daemons(redis)
        except RedisError as exc:
            await logger.awarning("daemon_registry_watch.poll_failed", error=str(exc))
        else:
            registered_by_id = {d.id: d for d in registered if d.enabled}
            for daemon_id, daemon in registered_by_id.items():
                if daemon_id not in manager.active_daemon_ids():
                    await manager.spawn(daemon)
                elif manager.current_config(daemon_id) != daemon:
                    await manager.stop(daemon_id)
                    await manager.spawn(daemon)
            removable = manager.active_daemon_ids() - yaml_daemon_ids - set(registered_by_id)
            for daemon_id in removable:
                await manager.stop(daemon_id)
        await asyncio.sleep(poll_interval_s)


async def run(
    settings: Settings, records_logger: RecordsLogger, redis: Redis | None = None
) -> None:
    """Wires every configured/registered daemon to its own `DaemonManager`-
    owned task and then blocks until cancelled (SIGTERM/SIGINT, via
    `__main__.py`). Daemon tasks are fire-and-forget background tasks
    (`DaemonManager.spawn`), not something this coroutine awaits directly
    -- unlike the pre-Phase-3 version, daemons can now be added after this
    call has already started.

    `redis`, if given, enables the runtime daemon registry watch (Phase 3);
    omitted (e.g. in a test that only cares about the YAML-configured set),
    this behaves like the static, boot-time-only version always did.
    """
    manager = DaemonManager(settings, records_logger)
    yaml_daemon_ids = frozenset(daemon.id for daemon in settings.enabled_daemons())
    for daemon in settings.enabled_daemons():
        await manager.spawn(daemon)
    if not yaml_daemon_ids and redis is None:
        await logger.awarning("app.no_enabled_daemons")

    await logger.ainfo("app.starting", daemon_ids=sorted(yaml_daemon_ids))
    if redis is None:
        await asyncio.Event().wait()
    else:
        await run_daemon_registry_watch(
            redis,
            manager,
            yaml_daemon_ids,
            poll_interval_s=settings.listener.daemon_registry_poll_interval_s,
        )
