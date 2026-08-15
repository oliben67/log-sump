import asyncio

from log_sump.common.config import DaemonConfig
from log_sump.common.transport import LocalTransport, SSHTransport
from log_sump.listener.app import ListenerManager, _build_new_container_dispatcher, build_transport
from log_sump.listener.registry import ContainerRef, Registry

from .conftest import FakeRecordsLogger, FakeTransport


def test_build_transport_local() -> None:
    daemon = DaemonConfig(id="dev", host="localhost", transport="local")
    assert isinstance(build_transport(daemon), LocalTransport)


def test_build_transport_ssh() -> None:
    daemon = DaemonConfig(id="prod", host="10.0.0.5", user="deploy", transport="ssh")
    transport = build_transport(daemon)
    assert isinstance(transport, SSHTransport)
    assert transport.command_prefix() == ["ssh", "deploy@10.0.0.5"]


async def test_spawn_tracks_active_container_and_stop_removes_it() -> None:
    listeners = ListenerManager(
        "daemon-a", FakeTransport(), FakeRecordsLogger(), asyncio.Semaphore(10)
    )
    ref = ContainerRef(container_id="c1", container_name="web")

    await listeners.spawn(ref)
    assert listeners.active_container_ids() == {"c1"}

    await listeners.stop("c1")
    assert listeners.active_container_ids() == set()


async def test_spawn_is_idempotent_for_an_already_active_container() -> None:
    listeners = ListenerManager(
        "daemon-a", FakeTransport(), FakeRecordsLogger(), asyncio.Semaphore(10)
    )
    ref = ContainerRef(container_id="c1", container_name="web")

    await listeners.spawn(ref)
    await listeners.spawn(ref)  # should not create a second task

    assert listeners.active_container_ids() == {"c1"}
    await listeners.stop("c1")


async def test_crashed_listener_is_removed_from_active_set_without_calling_stop() -> None:
    listeners = ListenerManager(
        "daemon-a",
        FakeTransport(raise_on_start=RuntimeError("connection refused")),
        FakeRecordsLogger(),
        asyncio.Semaphore(10),
    )
    ref = ContainerRef(container_id="c1", container_name="web")

    await listeners.spawn(ref)
    # Let the task run and fail on its own.
    await asyncio.sleep(0.05)

    assert listeners.active_container_ids() == set()


async def test_spawn_blocks_until_semaphore_capacity_is_available() -> None:
    semaphore = asyncio.Semaphore(0)  # fully exhausted
    listeners = ListenerManager("daemon-a", FakeTransport(), FakeRecordsLogger(), semaphore)
    ref = ContainerRef(container_id="c1", container_name="web")

    spawn_task = asyncio.create_task(listeners.spawn(ref))
    await asyncio.sleep(0.05)
    assert not spawn_task.done()
    assert listeners.active_container_ids() == set()

    semaphore.release()
    await asyncio.wait_for(spawn_task, timeout=1.0)
    assert listeners.active_container_ids() == {"c1"}

    await listeners.stop("c1")


async def test_dispatcher_routes_new_containers_to_the_correct_daemon() -> None:
    semaphore = asyncio.Semaphore(10)
    listeners_a = ListenerManager("daemon-a", FakeTransport(), FakeRecordsLogger(), semaphore)
    listeners_b = ListenerManager("daemon-b", FakeTransport(), FakeRecordsLogger(), semaphore)
    dispatcher = _build_new_container_dispatcher({"daemon-a": listeners_a, "daemon-b": listeners_b})

    registry = Registry()
    registry.on_new_container(dispatcher)

    web = ContainerRef(container_id="c1", container_name="web")
    db = ContainerRef(container_id="c2", container_name="db")

    await registry.update("daemon-a", {web})
    await registry.update("daemon-b", {db})

    assert listeners_a.active_container_ids() == {"c1"}
    assert listeners_b.active_container_ids() == {"c2"}

    await listeners_a.stop("c1")
    await listeners_b.stop("c2")
