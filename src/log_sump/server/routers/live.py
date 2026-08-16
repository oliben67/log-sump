"""GET /events: Server-Sent Events live-update stream (migration plan
Phase 6) -- ported from a prior gateway implementation's own `/events`,
unchanged in spirit: a lightweight "something changed, go re-fetch"
notification, not the actual new record content (see `broadcast.py`'s
module docstring). `{"type": "update", "docker_host": ...}` fires when new
data lands for a daemon (the ingestion consumer, a file upload);
`{"type": "catalog"}` fires when the daemon set itself changes
(`POST`/`DELETE /daemons`).

Auth: a browser's native `EventSource` can't attach a custom header at
all, by spec -- the same problem that prior implementation's own `/events`
had, and the same fix: an `api_key`/`token` query param is accepted here as
a fallback alongside the header, for either credential (see
`require_valid_api_key_or_gateway_token_sse`'s own docstring in
`deps.py` -- a daemon-scoped API key or the shared gateway token, either
one is enough).

The generator itself (`sse_generator`) is a standalone function, not a
closure inside the route handler, specifically so it can be tested
directly (bounded `anext()` calls) without going through a live HTTP
round-trip -- an indefinitely-running `StreamingResponse` body can't
practically be exercised end-to-end through `httpx`'s `ASGITransport`,
which (confirmed while building this) buffers a response's body to
completion before returning anything to the caller, status code included.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from ..broadcast import KEEPALIVE_INTERVAL_SECONDS, Broadcaster
from ..deps import get_broadcaster, require_valid_api_key_or_gateway_token_sse

router = APIRouter()


async def sse_generator(
    queue: asyncio.Queue,
    is_disconnected: Callable[[], Awaitable[bool]],
    *,
    keepalive_interval: float = KEEPALIVE_INTERVAL_SECONDS,
) -> AsyncGenerator[bytes, None]:
    """One SSE connection's body: an immediate `: connected` comment (lets
    a client tell "connected, nothing to report yet" apart from "still
    connecting"), then a published event as soon as one arrives, or a
    `: keepalive` comment every `keepalive_interval` seconds of silence --
    matches a prior gateway implementation's own `route_events`'s `gen()`.
    Ends (without raising) once `is_disconnected()` reports the client is
    gone; the caller is still responsible for unsubscribing `queue` from
    its `Broadcaster`.
    """
    yield b": connected\n\n"
    while True:
        if await is_disconnected():
            return
        try:
            event = await asyncio.wait_for(queue.get(), timeout=keepalive_interval)
            yield f"data: {json.dumps(event)}\n\n".encode()
        except TimeoutError:
            yield b": keepalive\n\n"


@router.get("/events", dependencies=[Depends(require_valid_api_key_or_gateway_token_sse)])
async def sse_events(
    request: Request, broadcaster: Annotated[Broadcaster, Depends(get_broadcaster)]
) -> StreamingResponse:
    queue = broadcaster.subscribe()

    async def gen() -> AsyncIterator[bytes]:
        try:
            async for chunk in sse_generator(queue, request.is_disconnected):
                yield chunk
        finally:
            broadcaster.unsubscribe(queue)

    return StreamingResponse(
        gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
    )
