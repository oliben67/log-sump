from log_sump.listener.container_tracker import ContainerTracker
from log_sump.listener.registry import ContainerRef, Registry


def _make_tracker(
    registry: Registry,
    docker_host: str,
    active: set[str],
    stopped: list[str],
    *,
    threshold: int = 3,
) -> ContainerTracker:
    async def stop_listener(container_id: str) -> None:
        stopped.append(container_id)
        active.discard(container_id)

    return ContainerTracker(
        registry,
        docker_host,
        active_container_ids=lambda: set(active),
        stop_listener=stop_listener,
        tracker_interval_s=0.01,
        missing_threshold_cycles=threshold,
    )


async def test_listener_survives_brief_absence_below_threshold() -> None:
    registry = Registry()
    web = ContainerRef(container_id="c1", container_name="web")
    await registry.update("daemon-a", {web})

    active = {"c1"}
    stopped: list[str] = []
    tracker = _make_tracker(registry, "daemon-a", active, stopped, threshold=3)

    await registry.update("daemon-a", set())
    await tracker.run_once()
    await registry.update("daemon-a", set())
    await tracker.run_once()

    assert stopped == []
    assert "c1" in active


async def test_listener_stopped_after_missing_threshold_cycles() -> None:
    registry = Registry()
    web = ContainerRef(container_id="c1", container_name="web")
    await registry.update("daemon-a", {web})

    active = {"c1"}
    stopped: list[str] = []
    tracker = _make_tracker(registry, "daemon-a", active, stopped, threshold=3)

    for _ in range(3):
        await registry.update("daemon-a", set())
    await tracker.run_once()

    assert stopped == ["c1"]
    assert "c1" not in active
    assert "c1" not in registry.state_for("daemon-a").containers


async def test_miss_count_resets_when_container_reappears() -> None:
    registry = Registry()
    web = ContainerRef(container_id="c1", container_name="web")
    await registry.update("daemon-a", {web})

    active = {"c1"}
    stopped: list[str] = []
    tracker = _make_tracker(registry, "daemon-a", active, stopped, threshold=3)

    await registry.update("daemon-a", set())
    await registry.update("daemon-a", set())
    await registry.update("daemon-a", {web})  # reappears -> miss count resets
    await registry.update("daemon-a", set())
    await registry.update("daemon-a", set())
    await tracker.run_once()

    assert stopped == []  # only 2 consecutive misses since reappearance


async def test_unknown_container_id_is_stopped_immediately() -> None:
    registry = Registry()
    active = {"ghost"}
    stopped: list[str] = []
    tracker = _make_tracker(registry, "daemon-a", active, stopped, threshold=3)

    await tracker.run_once()

    assert stopped == ["ghost"]
