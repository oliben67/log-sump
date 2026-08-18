"""Central registry (spec §5.1).

Holds, per daemon, the latest `docker ps` listing plus enough history for
liveness decisions: a monotonically increasing `listing_seq`, and per
container a `last_seen_seq`. Vanished containers are deliberately *not*
removed here on the first miss — that is `container_tracker`'s job, driven
by `missing_threshold_cycles`, so a single flaky `docker ps` cycle can't tear
down a healthy listener.

`update()` is async and invokes the registered callback directly for each
newly-discovered container, so a spawner reacts to changes as they happen
rather than polling registry state on its own loop.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

NewContainerCallback = Callable[[str, "ContainerRef"], Awaitable[None]]


@dataclass(frozen=True)
class ContainerRef:
    container_id: str
    container_name: str


@dataclass
class ContainerState:
    ref: ContainerRef
    last_seen_seq: int


@dataclass
class DaemonState:
    listing_seq: int = 0
    containers: dict[str, ContainerState] = field(default_factory=dict)


class Registry:
    """In-process shared state for every configured daemon.

    One instance per log-listener process. Safe across concurrent asyncio
    tasks on that single event loop: `update()` performs its bookkeeping
    with no `await` in between reads and writes, so it can't be interleaved
    with another task's call.
    """

    def __init__(self) -> None:
        self._daemons: dict[str, DaemonState] = {}
        self._on_new_container: NewContainerCallback | None = None

    def on_new_container(self, callback: NewContainerCallback) -> None:
        self._on_new_container = callback

    def state_for(self, docker_host: str) -> DaemonState:
        return self._daemons.setdefault(docker_host, DaemonState())

    async def update(self, docker_host: str, listing: set[ContainerRef]) -> None:
        """Record a fresh `docker ps` listing for `docker_host`.

        New containers fire the registered callback; already-known
        containers just get their `last_seen_seq` bumped to the current
        listing (which is also how a container's miss count resets to zero
        after reappearing, in `container_tracker`).
        """
        state = self.state_for(docker_host)
        state.listing_seq += 1
        new_refs: list[ContainerRef] = []
        for ref in listing:
            existing = state.containers.get(ref.container_id)
            state.containers[ref.container_id] = ContainerState(
                ref=ref, last_seen_seq=state.listing_seq
            )
            if existing is None:
                new_refs.append(ref)

        if self._on_new_container is not None:
            for ref in new_refs:
                await self._on_new_container(docker_host, ref)

    def forget(self, docker_host: str, container_id: str) -> None:
        """Drop a container once `container_tracker` has stopped its listener."""
        state = self._daemons.get(docker_host)
        if state is not None:
            state.containers.pop(container_id, None)

    def forget_daemon(self, docker_host: str) -> None:
        """Drops *all* known-container state for `docker_host` -- call this
        whenever that daemon's own task is stopped (`DaemonManager.stop`),
        not just when a single container's listener stops.

        Without this, a still-running container that was already known
        before the daemon stopped (e.g. `watched_containers` narrowed it
        out, so `_build_new_container_dispatcher` declined to spawn a
        listener for it) can never trigger `on_new_container` again after
        a respawn -- `update()` only fires that callback for a
        `container_id` it hasn't seen before, and this registry is shared
        across a daemon's entire lifetime, not recreated per spawn. A
        respawn (`run_daemon_registry_watch`, e.g. after `watched_containers`
        widens back out) is already documented as "a full stop+spawn... at
        the cost of a brief collection gap" -- this makes that actually
        true, instead of silently skipping anything the registry already
        knew about.
        """
        self._daemons.pop(docker_host, None)
