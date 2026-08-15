"""Migration plan Phase 6: Broadcaster -- direct translation of cttc's own
State.listeners/broadcast().
"""

import asyncio

from log_sump.server.broadcast import Broadcaster


def test_publish_with_no_subscribers_does_not_raise() -> None:
    Broadcaster().publish({"type": "update", "docker_host": "daemon-a"})


async def test_subscriber_receives_published_event() -> None:
    broadcaster = Broadcaster()
    queue = broadcaster.subscribe()

    broadcaster.publish({"type": "update", "docker_host": "daemon-a"})

    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert event == {"type": "update", "docker_host": "daemon-a"}


async def test_multiple_subscribers_all_receive_the_same_event() -> None:
    broadcaster = Broadcaster()
    queue_a = broadcaster.subscribe()
    queue_b = broadcaster.subscribe()

    broadcaster.publish({"type": "catalog"})

    assert await asyncio.wait_for(queue_a.get(), timeout=1.0) == {"type": "catalog"}
    assert await asyncio.wait_for(queue_b.get(), timeout=1.0) == {"type": "catalog"}


async def test_unsubscribe_stops_further_delivery() -> None:
    broadcaster = Broadcaster()
    queue = broadcaster.subscribe()
    broadcaster.unsubscribe(queue)

    broadcaster.publish({"type": "catalog"})

    assert queue.empty()


def test_unsubscribe_unknown_queue_does_not_raise() -> None:
    broadcaster = Broadcaster()
    Broadcaster().subscribe()  # a queue from a different broadcaster entirely
    broadcaster.unsubscribe(asyncio.Queue())


async def test_full_queue_drops_the_event_instead_of_blocking() -> None:
    from log_sump.server import broadcast as broadcast_module

    original_maxsize = broadcast_module.QUEUE_MAXSIZE
    broadcast_module.QUEUE_MAXSIZE = 1
    try:
        broadcaster = Broadcaster()
        queue = broadcaster.subscribe()
        broadcaster.publish({"type": "catalog"})  # fills the queue

        broadcaster.publish({"type": "catalog"})  # must not raise/block -- dropped

        assert queue.qsize() == 1
    finally:
        broadcast_module.QUEUE_MAXSIZE = original_maxsize
