"""Rolling metrics/log buffer feature (migration plan Phase 4). Direct
translation of cttc's own rolling_buffer.py -- "the last N minutes" of one
`docker_host`'s data, sliced lazily out of Streams at stop-time via
`queries.export_window`/`cttc_archive.write_archive` (same as
sessions.py). No source-id snapshot needed (see sessions.py's own
docstring for why cttc's own concept has no equivalent here): a buffer is
just `{docker_host, start_ts, minutes, paused_at}`.

Retention: an ad-hoc buffer (`start()`) left running longer than
`MAX_AGE_SECONDS` with no `stop()`/`pause()` (a bug, a crash, or just
forgetting) is reclaimed by `tick()`, and `start()` itself caps how many
can be open at once -- matches cttc's own `br-RBUF-005` fix. Neither
applies to a buffer `events.py` (Phase 5) keeps alive for an enabled
event's entire lifetime (`owned_by_event=True`) -- that lifetime is
bounded by the event itself (`cancel()`/`update()` always `stop()` it
first), not by age or count.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from redis.asyncio import Redis

from log_sump.common.cttc_archive import write_archive

from .queries import export_window

MAX_AGE_SECONDS = 24 * 3600.0  # generous enough for a normal "arm, come back later" session
MAX_OPEN = 200


class UnknownBuffer(KeyError):
    """Raised for an unknown/already-stopped buffer id."""


class TooManyBuffers(ValueError):
    """Raised by `start()` when `MAX_OPEN` buffers are already open."""


class BufferManager:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._buffers: dict[str, dict] = {}
        self._next_id = 1

    def start(self, docker_host: str, minutes: float, *, owned_by_event: bool = False) -> str:
        """Start a new buffer covering the last `minutes` minutes of
        `docker_host`, starting from now. Returns the new buffer's id.
        `events.py` passes `owned_by_event=True` to keep this buffer alive
        for its whole lifetime, exempt from the ad-hoc cap/TTL below (how
        many of those exist is already bounded by how many snapshot-action
        events exist).

        Raises `TooManyBuffers` if `MAX_OPEN` ad-hoc buffers are already
        open.
        """
        if not owned_by_event:
            open_ad_hoc = sum(1 for buf in self._buffers.values() if not buf["owned_by_event"])
            if open_ad_hoc >= MAX_OPEN:
                raise TooManyBuffers(
                    f"{MAX_OPEN} rolling buffers are already open -- "
                    "stop some before starting another"
                )
        buffer_id = f"b{self._next_id}"
        self._next_id += 1
        self._buffers[buffer_id] = {
            "docker_host": docker_host,
            "start_ts": time.time() * 1000.0,
            "minutes": minutes,
            "paused_at": None,
            "owned_by_event": owned_by_event,
        }
        return buffer_id

    def tick(self, now: float | None = None) -> list[str]:
        """Sweeps ad-hoc buffers open longer than `MAX_AGE_SECONDS` with no
        `stop()`. Never touches an `owned_by_event` buffer, meant to live
        as long as its event stays enabled, however long that is. Returns
        the ids reclaimed.
        """
        now = now if now is not None else time.time() * 1000.0
        expired = [
            bid
            for bid, buf in self._buffers.items()
            if not buf["owned_by_event"] and now - buf["start_ts"] > MAX_AGE_SECONDS * 1000.0
        ]
        for bid in expired:
            del self._buffers[bid]
        return expired

    def pause(self, buffer_id: str) -> None:
        """Stop admitting new data into the buffer's window -- its end
        time is frozen at the moment of this call, rather than "now" at
        `stop()`.
        """
        buf = self._require(buffer_id)
        if buf["paused_at"] is None:
            buf["paused_at"] = time.time() * 1000.0

    async def snapshot(self, buffer_id: str) -> bytes:
        """Like `stop()`, but leaves the buffer running."""
        buf = self._require(buffer_id)
        end = buf["paused_at"] if buf["paused_at"] is not None else time.time() * 1000.0
        t0 = max(buf["start_ts"], end - buf["minutes"] * 60_000.0)
        window = await export_window(
            self._redis,
            buf["docker_host"],
            datetime.fromtimestamp(t0 / 1000, tz=UTC),
            datetime.fromtimestamp(end / 1000, tz=UTC),
        )
        return write_archive(
            t0, end, window["log_sources"], window["stats_series"], window["swarm_services"]
        )

    async def stop(self, buffer_id: str) -> bytes:
        """Slice `[max(start_ts, end - minutes*60_000), end]` out of the
        buffer's `docker_host` and return it as `.cttc-metric` archive
        bytes, removing the buffer. `end` is the pause time if paused,
        else now.
        """
        data = await self.snapshot(buffer_id)
        del self._buffers[buffer_id]
        return data

    def _require(self, buffer_id: str) -> dict:
        buf = self._buffers.get(buffer_id)
        if buf is None:
            raise UnknownBuffer(buffer_id)
        return buf
