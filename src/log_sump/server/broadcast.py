"""In-process pub/sub for Server-Sent Events (migration plan Phase 6) --
ported from a prior gateway implementation's own `State.listeners`/
`broadcast()`: a lightweight "something changed, go re-fetch" notification
bus, not a full record-content stream. The actual data still comes from
the ordinary query endpoints (`/records`, `/series`, ...) -- an SSE event
only ever tells a client *that* something changed, never *what*.

Single-process only, matching every other piece of in-memory bookkeeping
in this codebase (sessions/buffers/scheduler/events are all in-process-
memory too, see their own module docstrings) -- there's no cross-worker
fan-out here, the same scope that prior implementation's own
`State.listeners` always had.
"""

from __future__ import annotations

import asyncio

#: How often a `: keepalive` comment is sent on an otherwise-idle
#: connection, so an intermediary proxy/load balancer doesn't time it out.
KEEPALIVE_INTERVAL_SECONDS = 15.0

#: A slow/stalled client drops events rather than applying backpressure to
#: every publisher.
QUEUE_MAXSIZE: int = 256


class Broadcaster:
    def __init__(self) -> None:
        self._listeners: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._listeners.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        if queue in self._listeners:
            self._listeners.remove(queue)

    def publish(self, event: dict) -> None:
        for queue in list(self._listeners):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass
