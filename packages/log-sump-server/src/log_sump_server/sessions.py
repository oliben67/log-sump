"""Recording sessions (migration plan Phase 4): on-demand or scheduler-
triggered captures that accumulate server-side and are collected by the
client afterward. Direct translation of cttc's own recording_session.py --
same "Redis already retains it, a session is just bookkeeping" design (see
that module's own docstring), scoped to one `docker_host` (log-sump's own
addressing unit) instead of an arbitrary set of "currently open sources"
(cttc's own concept, with no equivalent here: a daemon's Streams already
cover everything on it, so there's no separate source-id snapshot to keep
track of at all).

A session ends either by an explicit `stop()` call or once its planned
`duration_minutes` elapses (checked by `tick()`, a background task
alongside the ingestion consumer/trimmer in `app.py`'s lifespan).

Its completed archive (`cttc_archive.write_archive`) is stored in Redis
(`redis_keys.session_data_key`), not on local disk the way cttc's own
`sessions_dir` is -- log-server has no guaranteed persistent filesystem
across a restart the way the embedded gateway's own disk does, and Redis
is already this system's durable store for everything else. Only a
session's status/metadata bookkeeping stays in-process (matches cttc's own
single-process `_sessions` dict); unlike cttc, this module doesn't reclaim
orphaned archives after a restart -- a completed-but-undownloaded session's
data blob still exists in Redis until its own TTL, but its status becomes
unreachable once `_sessions` is gone. A smaller gap than it sounds: the
underlying Streams data a session was built from is untouched either way,
so nothing is actually lost, only that one convenience view of it.

Retention: completed sessions are erased `default_ttl_seconds` after they
finish (24h, matches cttc's own default), except sessions marked `safe`,
kept for their own `max_keep_seconds` instead.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from log_sump_common.cttc_archive import write_archive
from log_sump_common.redis_keys import session_data_key
from redis.asyncio import Redis

from .queries import export_window

logger = structlog.get_logger(__name__)

DEFAULT_TTL_SECONDS = 24 * 3600.0


class UnknownSession(KeyError):
    """Raised for an unknown, or not-yet-completed, session id."""


@dataclass
class RecordingSession:
    id: str
    docker_host: str
    start_ts: float  # epoch ms
    duration_minutes: float | None  # None -> ends only via an explicit stop()
    safe: bool = False
    max_keep_seconds: float | None = None
    status: str = "running"  # running -> completed
    end_ts: float | None = None
    stored_ts: float | None = None


class SessionManager:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._sessions: dict[str, RecordingSession] = {}
        self._next_id = 1
        self.default_ttl_seconds = DEFAULT_TTL_SECONDS

    def set_default_ttl(self, seconds: float) -> None:
        self.default_ttl_seconds = seconds

    def start(
        self,
        docker_host: str,
        *,
        duration_minutes: float | None = None,
        safe: bool = False,
        max_keep_seconds: float | None = None,
    ) -> str:
        """Begin a new session against `docker_host`, returning its id.
        `duration_minutes` of `None` means the session only ends via an
        explicit `stop()` (the on-demand case); `scheduling.py` always
        passes a duration.
        """
        sid = f"rec{self._next_id}"
        self._next_id += 1
        self._sessions[sid] = RecordingSession(
            id=sid,
            docker_host=docker_host,
            start_ts=time.time() * 1000.0,
            duration_minutes=duration_minutes,
            safe=safe,
            max_keep_seconds=max_keep_seconds,
        )
        return sid

    async def store_precomputed(
        self, data: bytes, *, safe: bool = False, max_keep_seconds: float | None = None
    ) -> str:
        """Register already-built archive bytes as a completed "session" --
        for `events.py` (Phase 5), whose triggered snapshots come from a
        rolling buffer's own `snapshot()` slice rather than from a session
        that ran on this manager's own clock. Gets the same id space,
        `download()`/`status_of()` access, and TTL handling as an ordinary
        session. Unlike cttc's own synchronous version (which just writes a
        local file), this is `async`: storing into Redis needs it.
        """
        sid = f"rec{self._next_id}"
        self._next_id += 1
        now = time.time() * 1000.0
        ttl_seconds = (
            max_keep_seconds if safe and max_keep_seconds is not None else self.default_ttl_seconds
        )
        await self._redis.set(session_data_key(sid), data, ex=max(1, int(ttl_seconds)))
        self._sessions[sid] = RecordingSession(
            id=sid,
            docker_host="",  # meaningless here, matches cttc's own empty source_ids placeholder
            start_ts=now,
            duration_minutes=None,
            safe=safe,
            max_keep_seconds=max_keep_seconds,
            status="completed",
            end_ts=now,
            stored_ts=now,
        )
        return sid

    def mark_safe(self, session_id: str, max_keep_seconds: float) -> None:
        """Flag a running or already-completed session as exempt from the
        default TTL sweep, kept instead for up to `max_keep_seconds` from
        completion.
        """
        sess = self._require(session_id)
        sess.safe = True
        sess.max_keep_seconds = max_keep_seconds

    async def stop(self, session_id: str) -> None:
        """End a running session now. A no-op if it's already completed."""
        sess = self._require(session_id)
        if sess.status == "running":
            await self._finish(sess, time.time() * 1000.0)

    async def tick(self, now: float | None = None) -> None:
        """Finish any running session whose planned duration has elapsed,
        then sweep expired completed sessions. Called periodically from
        `app.py`'s background loop.
        """
        now = now if now is not None else time.time() * 1000.0
        for sess in list(self._sessions.values()):
            if (
                sess.status == "running"
                and sess.duration_minutes is not None
                and now - sess.start_ts >= sess.duration_minutes * 60_000.0
            ):
                try:
                    await self._finish(sess, sess.start_ts + sess.duration_minutes * 60_000.0)
                except Exception:
                    logger.exception("session.finish_failed", session_id=sess.id)
        self._sweep(now)

    def status_of(self, session_id: str) -> dict:
        sess = self._require(session_id)
        return {
            "session_id": sess.id,
            "status": sess.status,
            "ready": sess.status == "completed",
            "safe": sess.safe,
        }

    async def download(self, session_id: str) -> bytes:
        sess = self._require(session_id)
        if sess.status != "completed":
            raise UnknownSession(session_id)
        data = await self._redis.get(session_data_key(session_id))
        if data is None:
            raise UnknownSession(session_id)  # already expired out of Redis
        # Always written as bytes (write_archive's own return type) -- the
        # str half of redis-py's return type only applies with
        # decode_responses=True, which this client never sets.
        assert isinstance(data, bytes)
        return data

    async def _finish(self, sess: RecordingSession, end_ts: float) -> None:
        t0 = datetime.fromtimestamp(sess.start_ts / 1000, tz=UTC)
        t1 = datetime.fromtimestamp(end_ts / 1000, tz=UTC)
        window = await export_window(self._redis, sess.docker_host, t0, t1)
        data = write_archive(
            sess.start_ts,
            end_ts,
            window["log_sources"],
            window["stats_series"],
            window["swarm_services"],
        )
        ttl_seconds = (
            sess.max_keep_seconds
            if sess.safe and sess.max_keep_seconds is not None
            else self.default_ttl_seconds
        )
        await self._redis.set(session_data_key(sess.id), data, ex=max(1, int(ttl_seconds)))
        sess.end_ts = end_ts
        sess.stored_ts = time.time() * 1000.0
        sess.status = "completed"

    def _sweep(self, now: float) -> list[str]:
        expired = []
        for sid, sess in self._sessions.items():
            if sess.status != "completed" or sess.stored_ts is None:
                continue
            ttl = (
                sess.max_keep_seconds
                if sess.safe and sess.max_keep_seconds is not None
                else self.default_ttl_seconds
            )
            if now - sess.stored_ts > ttl * 1000.0:
                expired.append(sid)
        for sid in expired:
            del self._sessions[sid]
        return expired

    def _require(self, session_id: str) -> RecordingSession:
        sess = self._sessions.get(session_id)
        if sess is None:
            raise UnknownSession(session_id)
        return sess
