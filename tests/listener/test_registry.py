from log_sump.listener.registry import ContainerRef, Registry


async def test_update_reports_only_newly_discovered_containers() -> None:
    registry = Registry()
    seen: list[tuple[str, ContainerRef]] = []

    async def on_new(docker_host: str, ref: ContainerRef) -> None:
        seen.append((docker_host, ref))

    registry.on_new_container(on_new)

    web = ContainerRef(container_id="c1", container_name="web")
    db = ContainerRef(container_id="c2", container_name="db")

    await registry.update("daemon-a", {web})
    assert seen == [("daemon-a", web)]

    await registry.update("daemon-a", {web, db})
    assert seen == [("daemon-a", web), ("daemon-a", db)]

    # Same set again -> no new containers reported.
    await registry.update("daemon-a", {web, db})
    assert len(seen) == 2


async def test_listing_seq_increments_and_last_seen_seq_tracks_presence() -> None:
    registry = Registry()
    web = ContainerRef(container_id="c1", container_name="web")

    await registry.update("daemon-a", {web})
    state = registry.state_for("daemon-a")
    assert state.listing_seq == 1
    assert state.containers["c1"].last_seen_seq == 1

    # Container absent from this cycle's listing: it stays in the registry
    # (container-tracker's job to remove it) but last_seen_seq doesn't advance.
    await registry.update("daemon-a", set())
    assert state.listing_seq == 2
    assert state.containers["c1"].last_seen_seq == 1

    # Reappears: last_seen_seq jumps back to the current listing_seq.
    await registry.update("daemon-a", {web})
    assert state.listing_seq == 3
    assert state.containers["c1"].last_seen_seq == 3


async def test_forget_removes_container_from_daemon_state() -> None:
    registry = Registry()
    web = ContainerRef(container_id="c1", container_name="web")
    await registry.update("daemon-a", {web})

    registry.forget("daemon-a", "c1")

    assert "c1" not in registry.state_for("daemon-a").containers


def test_state_for_is_isolated_per_daemon() -> None:
    registry = Registry()
    assert registry.state_for("daemon-a") is registry.state_for("daemon-a")
    assert registry.state_for("daemon-a") is not registry.state_for("daemon-b")
