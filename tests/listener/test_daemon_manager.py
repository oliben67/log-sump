"""Migration plan Phase 3: DaemonManager (the per-daemon ListenerManager
pattern applied one level up) and run_daemon_registry_watch, which
reconciles the runtime daemon registry against it.
"""

import asyncio

import pytest
from fakeredis import FakeAsyncRedis

from log_sump.common.config import DaemonConfig, Settings
from log_sump.common.daemon_registry import register_daemon, unregister_daemon
from log_sump.common.transport import ExecResult
from log_sump.listener import app as app_module
from log_sump.listener.app import DaemonManager, run_daemon_registry_watch

from .conftest import FakeRecordsLogger, FakeTransport


@pytest.fixture(autouse=True)
def _fake_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    # DaemonManager.spawn() builds a real Transport per daemon internally
    # (build_transport(daemon), unlike ListenerManager which takes an
    # already-built one) -- a real LocalTransport would shell out to an
    # actual `docker` binary against whatever (if any) daemon this sandbox
    # has, exactly what every other listener test avoids via FakeTransport.
    # An empty-but-successful result keeps containers_listing/container_
    # stats/services_listing (all spawned for real inside run_daemon) from
    # raising on their very first cycle.
    empty_result = ExecResult(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(
        app_module, "build_transport", lambda daemon: FakeTransport(run_result=empty_result)
    )


def _settings() -> Settings:
    return Settings(listener={"listing_interval_s": 1000.0, "tracker_interval_s": 1000.0})


async def test_daemon_manager_spawn_and_stop() -> None:
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")

    await manager.spawn(daemon)
    assert manager.active_daemon_ids() == {"daemon-a"}

    await manager.stop("daemon-a")
    assert manager.active_daemon_ids() == set()


async def test_daemon_manager_spawn_is_idempotent() -> None:
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")

    await manager.spawn(daemon)
    await manager.spawn(daemon)  # should not create a second task

    assert manager.active_daemon_ids() == {"daemon-a"}
    await manager.stop("daemon-a")


async def test_stop_forgets_the_daemon_in_the_shared_registry() -> None:
    """Without this, a container already known to the registry before this
    daemon stopped (e.g. watched_containers narrowed it out, so it was
    never dispatched to a listener) can never be rediscovered after a
    respawn -- Registry.update() only fires on_new_container for a
    container_id it hasn't seen before, and the registry is shared across
    a daemon's whole lifetime, not recreated per spawn.
    """
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")
    await manager.spawn(daemon)

    forgotten: list[str] = []
    manager._registry.forget_daemon = lambda docker_host: forgotten.append(docker_host)  # type: ignore[method-assign]

    await manager.stop("daemon-a")

    # Called from both stop() itself and the task's done-callback (stop()
    # cancels the task, which triggers it too) -- idempotent either way,
    # so what matters is it happened at all, for the right daemon.
    assert forgotten and set(forgotten) == {"daemon-a"}


async def test_current_config_reflects_the_last_spawned_config() -> None:
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    assert manager.current_config("daemon-a") is None  # never spawned

    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")
    await manager.spawn(daemon)
    assert manager.current_config("daemon-a") == daemon

    await manager.stop("daemon-a")
    assert manager.current_config("daemon-a") is None  # cleared on stop


async def _settle() -> None:
    await asyncio.sleep(0.05)


async def test_registry_watch_spawns_newly_registered_daemon() -> None:
    redis = FakeAsyncRedis()
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    watch_task = asyncio.create_task(
        run_daemon_registry_watch(redis, manager, frozenset(), poll_interval_s=0.02)
    )
    try:
        await _settle()
        assert manager.active_daemon_ids() == set()

        daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")
        await register_daemon(redis, daemon)
        await asyncio.sleep(0.1)

        assert manager.active_daemon_ids() == {"daemon-a"}
    finally:
        watch_task.cancel()
        for daemon_id in list(manager.active_daemon_ids()):
            await manager.stop(daemon_id)


async def test_registry_watch_stops_unregistered_daemon() -> None:
    redis = FakeAsyncRedis()
    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")
    await register_daemon(redis, daemon)
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    watch_task = asyncio.create_task(
        run_daemon_registry_watch(redis, manager, frozenset(), poll_interval_s=0.02)
    )
    try:
        await asyncio.sleep(0.1)
        assert manager.active_daemon_ids() == {"daemon-a"}

        await unregister_daemon(redis, "daemon-a")
        await asyncio.sleep(0.1)

        assert manager.active_daemon_ids() == set()
    finally:
        watch_task.cancel()


async def test_registry_watch_respawns_a_daemon_whose_watched_containers_changed() -> None:
    """Migration plan Phase 9 (selective collection): PATCH /daemons/{id}
    rewrites the registered DaemonConfig in place (same id) -- the watch
    loop must notice the config itself changed, not just id presence/
    absence, and respawn to pick up the new watched_containers.
    """
    redis = FakeAsyncRedis()
    daemon = DaemonConfig(id="daemon-a", host="10.0.0.1", transport="local")
    await register_daemon(redis, daemon)
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    watch_task = asyncio.create_task(
        run_daemon_registry_watch(redis, manager, frozenset(), poll_interval_s=0.02)
    )
    try:
        await asyncio.sleep(0.1)
        assert manager.current_config("daemon-a") == daemon

        updated = daemon.model_copy(update={"watched_containers": ["web"]})
        await register_daemon(redis, updated)  # same id -- an upsert, like PATCH does
        await asyncio.sleep(0.1)

        assert manager.active_daemon_ids() == {"daemon-a"}  # still just the one daemon
        assert manager.current_config("daemon-a") == updated
    finally:
        watch_task.cancel()
        for daemon_id in list(manager.active_daemon_ids()):
            await manager.stop(daemon_id)


async def test_registry_watch_never_stops_a_yaml_seeded_daemon() -> None:
    """Even if a YAML-seeded daemon is (as expected) absent from the
    registry, the watch loop must never touch it -- only ever removes what
    was itself registered through the admin API.
    """
    redis = FakeAsyncRedis()
    manager = DaemonManager(_settings(), FakeRecordsLogger())
    yaml_daemon = DaemonConfig(id="yaml-daemon", host="10.0.0.9", transport="local")
    await manager.spawn(yaml_daemon)
    watch_task = asyncio.create_task(
        run_daemon_registry_watch(
            redis, manager, frozenset({"yaml-daemon"}), poll_interval_s=0.02
        )
    )
    try:
        await asyncio.sleep(0.1)
        assert manager.active_daemon_ids() == {"yaml-daemon"}
    finally:
        watch_task.cancel()
        await manager.stop("yaml-daemon")


async def test_registry_watch_survives_redis_errors() -> None:
    from redis.exceptions import ConnectionError as RedisConnectionError

    class BoomRedis(FakeAsyncRedis):
        async def hgetall(self, *args: object, **kwargs: object) -> dict:
            raise RedisConnectionError("boom")

    manager = DaemonManager(_settings(), FakeRecordsLogger())
    watch_task = asyncio.create_task(
        run_daemon_registry_watch(BoomRedis(), manager, frozenset(), poll_interval_s=0.02)
    )
    await asyncio.sleep(0.1)
    assert not watch_task.done()  # kept looping, didn't crash
    watch_task.cancel()
