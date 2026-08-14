"""Gateway-hosted events (migration plan Phase 5): watch metrics/logs on a
`docker_host` and, when one or more conditions are met, take a snapshot or
start a recording -- no polling by the client required. Direct translation
of cttc's own events.py, scoped to a single `docker_host` (log-sump's own
addressing unit) instead of an arbitrary "currently open sources" set --
same reasoning as `sessions.py`/`buffers.py`.

An event can carry more than one condition; `match` picks whether *any*
one of them firing is enough (default) or *all* must be true at once. A
metric condition (cpu/mem/net + comparison + threshold) is checked against
the *latest* sample of every container on the monitored daemon; a log
condition (a regex) is checked against every new log line since the event
was created or last checked -- addressed by a Stream ID cursor, one per
condition (not per-source like cttc: a `docker_host` here has a single
shared log stream, not per-source zsets, so there's no source-keyed
sub-dict needed).

The snapshot action needs "the records on hand" the moment a condition
fires, including a bit of *before* the trigger -- exactly what
`buffers.py` already provides, so `create()` starts one (kept alive for
the event's whole life, `owned_by_event=True`) and a trigger just calls
its non-destructive `snapshot()`. A recording action starts an ordinary
forward-looking `sessions.py` session instead. Either way the result is
handed to `SessionManager.store_precomputed()`/`start()`, so it gets the
same TTL/safe-flag handling and the same `/session/*` download endpoints
as any other snapshot or recording.

An event keeps watching for as long as it's enabled -- `disable()`/
`cancel()` are the only things that stop it. Firing is edge-triggered:
`_armed` tracks whether the condition was *not* met on the previous check;
only a not-met -> met transition fires, so a condition that stays true
doesn't re-snapshot/re-record on every tick. `reset()` forces the latch
back to "ready" without waiting for the condition to actually clear.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Literal

import structlog
from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind, MetricRecord
from redis.asyncio import Redis

from .buffers import BufferManager, UnknownBuffer
from .queries import _decode, _decode_entry, _metric_group, _ts_ms
from .sessions import SessionManager

logger = structlog.get_logger(__name__)

_OPS = {
    ">": lambda v, t: v > t,
    "<": lambda v, t: v < t,
    ">=": lambda v, t: v >= t,
    "<=": lambda v, t: v <= t,
    "=": lambda v, t: v == t,
}
#: How many of a daemon's most recent metric entries to scan per condition
#: check -- enough to find every currently-active container's latest (and,
#: for a net condition, second-latest) sample without walking history.
_METRIC_CHECK_WINDOW = 400


class UnknownEvent(KeyError):
    """Raised for an unknown event id."""


class InvalidEvent(ValueError):
    """Raised for a malformed condition/action at create()/update() time."""


@dataclass
class MetricCondition:
    metric: Literal["cpu", "mem", "net"]
    op: Literal[">", "<", ">=", "<=", "="]
    threshold: float


@dataclass
class LogCondition:
    pattern: str  # regex, matched against each new log line's message


@dataclass
class Action:
    kind: Literal["snapshot", "recording"]
    minutes: float | None = None  # snapshot: the rolling buffer's window length
    duration_minutes: float | None = None  # recording: how long to record for
    safe: bool = False
    max_keep_seconds: float | None = None


@dataclass
class Event:
    id: str
    name: str
    docker_host: str
    conditions: list[MetricCondition | LogCondition]
    match: Literal["any", "all"]
    action: Action
    enabled: bool = True
    status: str = "armed"  # armed (watching) <-> triggered (latched until condition clears)
    buffer_id: str | None = None  # snapshot actions only: the backing rolling buffer
    triggered_at: float | None = None
    artifact_id: str | None = None  # the most recent trigger's session_id
    trigger_detail: str | None = None
    trigger_count: int = 0
    _armed: bool = True  # edge-trigger latch -- see module docstring
    # condition index -> last Stream ID checked (log conditions only)
    _log_cursors: dict[int, str | None] = field(default_factory=dict)


def _validate(
    conditions: list[MetricCondition | LogCondition], match: str, action: Action
) -> None:
    if not conditions:
        raise InvalidEvent("an event needs at least one condition")
    if match not in ("any", "all"):
        raise InvalidEvent(f"match must be 'any' or 'all', got {match!r}")
    for condition in conditions:
        if isinstance(condition, MetricCondition):
            if condition.metric not in ("cpu", "mem", "net"):
                raise InvalidEvent(f"unknown metric: {condition.metric}")
            if condition.op not in _OPS:
                raise InvalidEvent(f"unknown operator: {condition.op}")
        elif isinstance(condition, LogCondition):
            try:
                re.compile(condition.pattern)
            except re.error as e:
                raise InvalidEvent(f"invalid regex: {condition.pattern}") from e
        else:
            raise InvalidEvent(f"unknown condition type: {condition!r}")
    if action.kind == "snapshot":
        if not action.minutes:
            raise InvalidEvent("a snapshot action needs `minutes`")
    elif action.kind == "recording":
        if not action.duration_minutes:
            raise InvalidEvent("a recording action needs `duration_minutes`")
    else:
        raise InvalidEvent(f"unknown action kind: {action.kind}")


class EventManager:
    def __init__(self, redis: Redis, buffers: BufferManager, sessions: SessionManager) -> None:
        self._redis = redis
        self._buffers = buffers
        self._sessions = sessions
        self._events: dict[str, Event] = {}
        self._next_id = 1

    async def create(
        self,
        name: str,
        docker_host: str,
        conditions: list[MetricCondition | LogCondition],
        action: Action,
        match: Literal["any", "all"] = "any",
    ) -> str:
        _validate(conditions, match, action)
        eid = f"evt{self._next_id}"
        self._next_id += 1
        buffer_id = None
        if action.kind == "snapshot":
            assert action.minutes is not None  # guaranteed by _validate
            buffer_id = self._buffers.start(docker_host, action.minutes, owned_by_event=True)
        self._events[eid] = Event(
            id=eid,
            name=name,
            docker_host=docker_host,
            conditions=list(conditions),
            match=match,
            action=action,
            buffer_id=buffer_id,
            _log_cursors=await self._seed_log_cursors(docker_host, conditions),
        )
        return eid

    async def update(
        self,
        event_id: str,
        *,
        name: str | None = None,
        docker_host: str | None = None,
        conditions: list[MetricCondition | LogCondition] | None = None,
        action: Action | None = None,
        match: Literal["any", "all"] | None = None,
    ) -> None:
        """Change an existing event in place (same id, same trigger
        history). Any argument left as `None` keeps that field unchanged.
        Changing `action` to/from a snapshot, or changing `docker_host`
        while it's a snapshot action, restarts its backing rolling buffer.
        """
        ev = self._require(event_id)
        new_conditions = conditions if conditions is not None else ev.conditions
        new_match = match if match is not None else ev.match
        new_action = action if action is not None else ev.action
        _validate(new_conditions, new_match, new_action)

        new_docker_host = docker_host if docker_host is not None else ev.docker_host
        was_snapshot = ev.action.kind == "snapshot"
        will_be_snapshot = new_action.kind == "snapshot"
        needs_restart = will_be_snapshot and (
            action is not None or docker_host is not None or not was_snapshot
        )
        if needs_restart or (was_snapshot and not will_be_snapshot):
            if ev.buffer_id is not None:
                try:
                    await self._buffers.stop(ev.buffer_id)
                except UnknownBuffer:
                    # already stopped/expired on its own -- fine, proceed to replace it.
                    logger.debug(
                        "events.update_buffer_already_gone",
                        event_id=event_id,
                        buffer_id=ev.buffer_id,
                    )
                ev.buffer_id = None
            if will_be_snapshot:
                assert new_action.minutes is not None
                ev.buffer_id = self._buffers.start(
                    new_docker_host, new_action.minutes, owned_by_event=True
                )

        if name is not None:
            ev.name = name
        if docker_host is not None:
            ev.docker_host = new_docker_host
        if conditions is not None:
            ev.conditions = list(new_conditions)
            ev._log_cursors = await self._seed_log_cursors(new_docker_host, new_conditions)
        ev.match = new_match
        ev.action = new_action

    async def _seed_log_cursors(
        self, docker_host: str, conditions: list[MetricCondition | LogCondition]
    ) -> dict[int, str | None]:
        """A log condition only watches for lines appended *after* it
        starts being watched -- seed each condition's cursor at the
        stream's current tip so pre-existing backlog never counts as a
        fresh match.
        """
        stream = stream_key(docker_host, Kind.LOG)
        top = await self._redis.xrevrange(stream, count=1)
        top_id = top[0][0] if top else None
        cursor = _decode(top_id) if top_id is not None else None
        return {
            i: cursor for i, cond in enumerate(conditions) if isinstance(cond, LogCondition)
        }

    async def cancel(self, event_id: str) -> None:
        ev = self._require(event_id)
        if ev.buffer_id is not None:
            try:
                await self._buffers.stop(ev.buffer_id)
            except UnknownBuffer:
                logger.debug(
                    "events.cancel_buffer_already_gone", event_id=event_id, buffer_id=ev.buffer_id
                )
        del self._events[event_id]

    def enable(self, event_id: str) -> None:
        self._require(event_id).enabled = True

    def disable(self, event_id: str) -> None:
        self._require(event_id).enabled = False

    def reset(self, event_id: str) -> None:
        """Force the edge-trigger latch back to "ready" without waiting
        for the condition to actually clear first.
        """
        ev = self._require(event_id)
        ev._armed = True
        ev.status = "armed"

    def status_of(self, event_id: str) -> dict:
        ev = self._require(event_id)
        return {
            "event_id": ev.id,
            "name": ev.name,
            "docker_host": ev.docker_host,
            "conditions": [self._condition_json(c) for c in ev.conditions],
            "match": ev.match,
            "action": {
                "kind": ev.action.kind,
                "minutes": ev.action.minutes,
                "duration_minutes": ev.action.duration_minutes,
                "safe": ev.action.safe,
                "max_keep_seconds": ev.action.max_keep_seconds,
            },
            "enabled": ev.enabled,
            "status": ev.status,
            "triggered_at": ev.triggered_at,
            "artifact_id": ev.artifact_id,
            "trigger_detail": ev.trigger_detail,
            "trigger_count": ev.trigger_count,
        }

    @staticmethod
    def _condition_json(cond: MetricCondition | LogCondition) -> dict:
        if isinstance(cond, MetricCondition):
            return {
                "type": "metric",
                "metric": cond.metric,
                "op": cond.op,
                "threshold": cond.threshold,
            }
        return {"type": "log", "pattern": cond.pattern}

    def list_ids(self) -> list[str]:
        return list(self._events.keys())

    async def tick(self, now: float | None = None) -> None:
        now = now if now is not None else time.time() * 1000.0
        for ev in list(self._events.values()):
            if not ev.enabled:
                continue
            try:
                detail = await self._check(ev)
                if detail is not None:
                    if ev._armed:
                        await self._fire(ev, detail, now)
                    ev.status = "triggered"
                else:
                    ev._armed = True
                    ev.status = "armed"
            except Exception:
                logger.exception("events.tick_failed", event_id=ev.id)

    async def _check(self, ev: Event) -> str | None:
        # every condition is always evaluated (never short-circuited), so a
        # log condition's read cursor keeps advancing each tick regardless
        # of `match` or of the edge-trigger latch's own state
        details = [await self._check_one(ev, i, cond) for i, cond in enumerate(ev.conditions)]
        hits = [d for d in details if d is not None]
        if ev.match == "any":
            return hits[0] if hits else None
        if len(hits) == len(ev.conditions):
            return "; ".join(hits)
        return None

    async def _check_one(
        self, ev: Event, index: int, cond: MetricCondition | LogCondition
    ) -> str | None:
        if isinstance(cond, MetricCondition):
            return await self._check_metric(ev, cond)
        return await self._check_log(ev, index, cond)

    async def _check_metric(self, ev: Event, cond: MetricCondition) -> str | None:
        cmp = _OPS[cond.op]
        stream = stream_key(ev.docker_host, Kind.METRIC)
        raw_entries = await self._redis.xrevrange(stream, count=_METRIC_CHECK_WINDOW)
        # container_id -> its two most recent samples (second one only
        # needed for a net condition's rate -- see _metric_value).
        recent_by_container: dict[str, list[tuple[int, MetricRecord]]] = {}
        for entry_id, fields in raw_entries or []:
            if entry_id is None:
                continue
            record = _decode_entry(fields)
            if not isinstance(record, MetricRecord):
                continue
            bucket = recent_by_container.setdefault(record.container_id, [])
            if len(bucket) < 2:
                bucket.append((_ts_ms(entry_id), record))

        for samples in recent_by_container.values():
            val = self._metric_value(cond.metric, samples)
            if val is not None and cmp(val, cond.threshold):
                _ts, latest = samples[0]
                group, _is_service = _metric_group(latest.container_name)
                return f"{group}: {cond.metric}={val} {cond.op} {cond.threshold}"
        return None

    @staticmethod
    def _metric_value(metric: str, samples: list[tuple[int, MetricRecord]]) -> float | None:
        _latest_ts, latest = samples[0]
        if metric == "cpu":
            return latest.cpu_pct
        if metric == "mem":
            return latest.mem_pct
        # net: no cumulative-counter equivalent in a single sample -- derive
        # a rate from this container's own two most recent samples (a
        # different container's counters are unrelated, same reasoning as
        # queries.bucketed/export_window).
        if latest.net_rx_bytes is None or latest.net_tx_bytes is None or len(samples) < 2:
            return None
        latest_ts, _ = samples[0]
        prev_ts, prev = samples[1]
        if prev.net_rx_bytes is None or prev.net_tx_bytes is None or latest_ts <= prev_ts:
            return None
        latest_total = latest.net_rx_bytes + latest.net_tx_bytes
        prev_total = prev.net_rx_bytes + prev.net_tx_bytes
        delta = latest_total - prev_total
        if delta < 0:  # counter reset (container restart)
            return None
        return delta / ((latest_ts - prev_ts) / 1000.0)

    async def _check_log(self, ev: Event, index: int, cond: LogCondition) -> str | None:
        pattern = re.compile(cond.pattern)
        cursor = ev._log_cursors.get(index)
        stream = stream_key(ev.docker_host, Kind.LOG)
        min_bound = f"({cursor}" if cursor else "-"
        raw_entries = await self._redis.xrange(stream, min=min_bound, max="+")
        hit = None
        last_id = cursor
        for entry_id, fields in raw_entries or []:
            if entry_id is None:
                continue
            last_id = _decode(entry_id)
            record = _decode_entry(fields)
            if record is None:
                continue
            message = str(getattr(record, "message", ""))
            if hit is None and pattern.search(message):
                hit = f"{record.container_id}: matched {message!r}"
        ev._log_cursors[index] = last_id
        return hit

    async def _fire(self, ev: Event, detail: str, now: float) -> None:
        ev._armed = False
        ev.triggered_at = now
        ev.trigger_detail = detail
        ev.trigger_count += 1
        if ev.action.kind == "snapshot":
            assert ev.buffer_id is not None
            data = await self._buffers.snapshot(ev.buffer_id)
            ev.artifact_id = await self._sessions.store_precomputed(
                data, safe=ev.action.safe, max_keep_seconds=ev.action.max_keep_seconds
            )
        else:
            assert ev.action.duration_minutes is not None
            ev.artifact_id = self._sessions.start(
                ev.docker_host,
                duration_minutes=ev.action.duration_minutes,
                safe=ev.action.safe,
                max_keep_seconds=ev.action.max_keep_seconds,
            )

    def _require(self, event_id: str) -> Event:
        ev = self._events.get(event_id)
        if ev is None:
            raise UnknownEvent(event_id)
        return ev
