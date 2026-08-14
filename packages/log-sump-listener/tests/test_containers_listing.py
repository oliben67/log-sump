import asyncio

import pytest
from log_sump_common.transport import ExecResult, Transport, TransportError
from log_sump_listener.containers_listing import _list_containers, run_containers_listing
from log_sump_listener.registry import ContainerRef, Registry

from .conftest import FakeTransport

DOCKER_PS_OUTPUT = '{"ID":"c1","Names":"web-1"}\n{"ID":"c2","Names":"db-1"}\n'


async def test_list_containers_parses_json_lines() -> None:
    transport = FakeTransport(
        run_result=ExecResult(returncode=0, stdout=DOCKER_PS_OUTPUT, stderr="")
    )
    refs = await _list_containers(transport)
    assert refs == {
        ContainerRef(container_id="c1", container_name="web-1"),
        ContainerRef(container_id="c2", container_name="db-1"),
    }


async def test_list_containers_raises_on_nonzero_exit() -> None:
    transport = FakeTransport(
        run_result=ExecResult(returncode=1, stdout="", stderr="daemon unreachable")
    )
    with pytest.raises(TransportError):
        await _list_containers(transport)


async def _run_one_cycle(transport: Transport, registry: Registry) -> list[tuple[bool, int]]:
    cycles: list[tuple[bool, int]] = []

    async def on_cycle(reachable: bool, count: int) -> None:
        cycles.append((reachable, count))

    task = asyncio.create_task(
        run_containers_listing(
            "daemon-a", transport, registry, listing_interval_s=10.0, on_cycle=on_cycle
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    return cycles


async def test_run_containers_listing_updates_registry_and_reports_success() -> None:
    transport = FakeTransport(
        run_result=ExecResult(returncode=0, stdout=DOCKER_PS_OUTPUT, stderr="")
    )
    registry = Registry()

    cycles = await _run_one_cycle(transport, registry)

    assert cycles == [(True, 2)]
    assert registry.state_for("daemon-a").listing_seq == 1


async def test_run_containers_listing_survives_transport_errors() -> None:
    transport = FakeTransport(raise_on_start=TransportError("connection refused"))
    registry = Registry()

    cycles = await _run_one_cycle(transport, registry)

    assert cycles == [(False, 0)]
    # listing_seq never advanced, and the loop kept running instead of crashing.
    assert registry.state_for("daemon-a").listing_seq == 0
