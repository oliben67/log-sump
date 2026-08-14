"""XRANGE-based query helpers for `GET /records` (spec §5.5).

Time-range and pagination boundaries are Redis Stream IDs, either derived
from a `start`/`end` datetime or an explicit per-kind cursor returned by a
previous page. The `:log` and `:metric` streams are queried and paginated
**independently** — each with its own cursor — then merged and sorted by
each record's own `ts` field for the "interleaved by ts" contiguous-
timeline requirement, not by Redis Stream ID (which is ingestion order and
can drift slightly from event time under load).

Deliberately *not* trying to enforce one combined `limit` across both
streams merged together: fetching up to `limit` from each kind and
returning the union (so a two-kind page can hold up to `2 * limit` records)
keeps each kind's cursor advancing exactly past what was actually fetched
for that kind. A joint "top `limit` after merging, discard the rest" scheme
would need each kind's cursor to sometimes stop *before* what was fetched
(so the discarded remainder isn't skipped next page) and sometimes advance
past a kind that contributed nothing to a page's truncation window purely
by time-ordering luck — solvable, but a lot more bookkeeping for a
guarantee ("exactly `limit` records per response") that isn't actually a
hard requirement here.
"""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict

from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import Kind, LogRecord, MetricRecord, RecordAdapter
from pydantic import ValidationError
from redis.asyncio import Redis

#: Redis Stream IDs are `<ms>-<seq>`; this is the max seq for a given ms,
#: so `<ms>-MAX_SEQ` is an inclusive upper bound covering every entry
#: stamped within that millisecond.
_MAX_STREAM_SEQ = (2**64) - 1


def _id_floor(dt: datetime) -> str:
    return f"{int(dt.timestamp() * 1000)}-0"


def _id_ceiling(dt: datetime) -> str:
    return f"{int(dt.timestamp() * 1000)}-{_MAX_STREAM_SEQ}"


async def fetch_kind_page(
    redis: Redis,
    docker_host: str,
    kind: Kind,
    *,
    start: datetime,
    end: datetime,
    cursor: str | None,
    limit: int,
) -> tuple[list[tuple[str, LogRecord | MetricRecord]], str | None]:
    """One kind's page: `(entries, next_cursor)`.

    `next_cursor` is `None` once a fetch comes back short of `limit` —
    there's nothing left in range for this kind (for now; new records can
    still arrive later, but that's true of any tail-following pagination).
    """
    range_start = f"({cursor}" if cursor else _id_floor(start)
    raw_entries = (
        await redis.xrange(
            stream_key(docker_host, kind), min=range_start, max=_id_ceiling(end), count=limit
        )
        or []
    )

    entries: list[tuple[str, LogRecord | MetricRecord]] = []
    for entry_id, fields in raw_entries:
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if record is None:
            continue
        entries.append((_decode(entry_id), record))

    next_cursor = entries[-1][0] if len(raw_entries) == limit else None
    return entries, next_cursor


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_entry(fields: dict | None) -> LogRecord | MetricRecord | None:
    """One `XRANGE`/`XREVRANGE` entry's `{"data": <json>}` fields -> a
    validated Record, or `None` for a hole. Shared by every reader below
    (and `fetch_kind_page` above) so "lenient on read, already validated
    once by the consumer" is enforced in exactly one place.
    """
    if fields is None:
        return None
    data = fields.get(b"data")
    if data is None:
        return None
    try:
        return RecordAdapter.validate_json(data)
    except ValidationError:
        return None


def _ts_ms(entry_id: bytes | str) -> int:
    """The millisecond timestamp encoded in a Stream ID's own `<ms>-<seq>`
    shape. Used for bucketing/nearest-neighbor math below instead of each
    record's own `ts` field, consistent with `_id_floor`/`_id_ceiling`
    already assuming Stream ID <-> event time closeness for range queries
    (see their use in `fetch_kind_page`) — one notion of "when", not two
    that could disagree.
    """
    return int(_decode(entry_id).split("-", 1)[0])


#: Bound on how many entries `point_at`/`index_at` scan on each side of the
#: target time. Generous headroom for "every container's latest sample
#: around this instant" (metrics land one per container per poll interval)
#: without walking unrelated history.
_NEAREST_WINDOW = 200


async def point_at(
    redis: Redis, docker_host: str, kind: Kind, t: datetime, *, window: int = _NEAREST_WINDOW
) -> dict[str, tuple[str, LogRecord | MetricRecord]]:
    """Per `container_id`, the entry whose Stream ID timestamp is closest to
    `t` — cttc's `/point`: compare an arbitrary instant (e.g. a loaded
    sample) against another (e.g. live "now"), across every container
    visible on this daemon at once, not scoped to a single one.
    """
    target_ms = int(t.timestamp() * 1000)
    stream = stream_key(docker_host, kind)
    before = await redis.xrevrange(stream, max=_id_ceiling(t), count=window) or []
    after = await redis.xrange(stream, min=f"({_id_ceiling(t)}", count=window) or []

    best: dict[str, tuple[str, LogRecord | MetricRecord, int]] = {}
    for entry_id, fields in (*before, *after):
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if record is None:
            continue
        distance = abs(_ts_ms(entry_id) - target_ms)
        current = best.get(record.container_id)
        if current is None or distance < current[2]:
            best[record.container_id] = (_decode(entry_id), record, distance)
    return {cid: (entry_id, record) for cid, (entry_id, record, _distance) in best.items()}


async def index_at(
    redis: Redis,
    docker_host: str,
    container_id: str,
    t: datetime,
    *,
    window: int = _NEAREST_WINDOW,
) -> str | None:
    """The Stream ID of `container_id`'s log entry at-or-before `t`, or the
    nearest one after if none precede it — cttc's `LogSource.index_at`.
    There it's an integer rank into that container's own zset; here it's a
    Stream ID cursor instead (see `find_text`'s docstring for why Stream-ID
    addressing replaces the integer-rank scheme log-sump has no O(1) way to
    produce over a stream shared by every container on the daemon).
    """
    stream = stream_key(docker_host, Kind.LOG)
    before = await redis.xrevrange(stream, max=_id_ceiling(t), count=window) or []
    for entry_id, fields in before:
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if record is not None and record.container_id == container_id:
            return _decode(entry_id)
    after = await redis.xrange(stream, min=f"({_id_ceiling(t)}", count=window) or []
    for entry_id, fields in after:
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if record is not None and record.container_id == container_id:
            return _decode(entry_id)
    return None


async def ticks(
    redis: Redis,
    docker_host: str,
    container_id: str,
    kind: Kind,
    t0: datetime,
    t1: datetime,
    px: int,
) -> list[int]:
    """Event-density strip: count of `container_id`'s entries per pixel
    bucket across `[t0, t1]` — cttc's `/ticks`.

    Streams are split by `(docker_host, kind)`, not by container
    (`redis_keys.stream_key`'s own docstring explains why: container IDs
    churn on every redeploy), so this scans every entry in the daemon's
    stream for the requested range and filters by `container_id`
    client-side — O(daemon's total volume in range), not O(this
    container's own volume). Accepted cost of that per-daemon layout (see
    the migration plan's non-negotiables); cttc's own `LogSource.ticks`
    pays no such cost only because its store keeps one zset per container.
    """
    px = max(1, px)
    dt_ms = max(1.0, (t1 - t0).total_seconds() * 1000.0 / px)
    t0_ms = int(t0.timestamp() * 1000)
    counts = [0] * px
    raw_entries = await redis.xrange(
        stream_key(docker_host, kind), min=_id_floor(t0), max=_id_ceiling(t1)
    )
    for entry_id, fields in raw_entries or []:
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if record is None or record.container_id != container_id:
            continue
        bucket = int((_ts_ms(entry_id) - t0_ms) / dt_ms)
        if 0 <= bucket < px:
            counts[bucket] += 1
    return counts


class BucketedContainer(TypedDict):
    container_id: str
    name: str
    cpu: list[float | None]
    mem: list[float | None]
    net: list[float | None]


async def bucketed(
    redis: Redis, docker_host: str, t0: datetime, t1: datetime, px: int
) -> list[BucketedContainer]:
    """Per container, per pixel bucket: max `cpu_pct`, max `mem_pct`, max
    net bytes/sec — cttc's `StatsSource.bucketed` (backs `/series`).

    `MetricRecord` stores net as cumulative rx+tx counters, matching
    `docker stats`' own semantics (see `architecture.md`), so the rate is
    derived here from each container's own consecutive samples — sorted by
    Stream ID timestamp first — before bucketing. Same two-step cttc's
    `StatsSource._net_rate` does, just performed at query time instead of
    ingest time (a counter decrease, e.g. a container restart, is treated
    the same way: skipped rather than yielding a negative rate).
    """
    px = max(1, px)
    dt_ms = max(1.0, (t1 - t0).total_seconds() * 1000.0 / px)
    t0_ms = int(t0.timestamp() * 1000)
    raw_entries = await redis.xrange(
        stream_key(docker_host, Kind.METRIC), min=_id_floor(t0), max=_id_ceiling(t1)
    )

    by_container: dict[str, list[tuple[int, MetricRecord]]] = {}
    for entry_id, fields in raw_entries or []:
        if entry_id is None:
            continue
        record = _decode_entry(fields)
        if not isinstance(record, MetricRecord):
            continue
        by_container.setdefault(record.container_id, []).append((_ts_ms(entry_id), record))

    out: list[BucketedContainer] = []
    for container_id, samples in by_container.items():
        samples.sort(key=lambda pair: pair[0])
        cpu: list[float | None] = [None] * px
        mem: list[float | None] = [None] * px
        net: list[float | None] = [None] * px
        prev_total: tuple[int, int] | None = None  # (ts_ms, rx+tx bytes)
        for ts_ms, record in samples:
            bucket = int((ts_ms - t0_ms) / dt_ms)
            in_range = 0 <= bucket < px

            current_cpu = cpu[bucket] if in_range else None
            if in_range and record.cpu_pct is not None:
                if current_cpu is None or record.cpu_pct > current_cpu:
                    cpu[bucket] = record.cpu_pct
            current_mem = mem[bucket] if in_range else None
            if in_range and record.mem_pct is not None:
                if current_mem is None or record.mem_pct > current_mem:
                    mem[bucket] = record.mem_pct

            total = None
            if record.net_rx_bytes is not None and record.net_tx_bytes is not None:
                total = record.net_rx_bytes + record.net_tx_bytes
            if total is not None:
                if prev_total is not None and ts_ms > prev_total[0]:
                    delta = total - prev_total[1]
                    if delta >= 0:  # negative == counter reset (restart); skip, matches cttc
                        rate = delta / ((ts_ms - prev_total[0]) / 1000.0)
                        current_net = net[bucket] if in_range else None
                        if in_range and (current_net is None or rate > current_net):
                            net[bucket] = rate
                prev_total = (ts_ms, total)

        out.append(
            BucketedContainer(
                container_id=container_id,
                name=samples[0][1].container_name,
                cpu=cpu,
                mem=mem,
                net=net,
            )
        )
    return out


#: Bound on how many entries find_text scans per direction before giving up
#: -- a full-text index is out of scope here (cttc's own LogSource.find has
#: none either; this only adds a per-daemon-stream filter cost on top of
#: the same linear-scan approach, see `ticks`'s docstring).
_FIND_PAGE = 500
_FIND_MAX_SCANNED = 5000


async def find_text(
    redis: Redis,
    docker_host: str,
    container_id: str,
    query: str,
    *,
    cursor: str | None,
    forward: bool = True,
) -> str | None:
    """Case-insensitive substring search over `container_id`'s log entries,
    wrapping around once the search direction runs off the end — cttc's
    `LogSource.find`, addressed by Stream ID cursor instead of an integer
    row index: log-sump has no O(1) per-container rank to resume from (its
    streams are shared by every container on the daemon), and a Stream ID
    cursor matches the pagination idiom `/records` already uses.
    """
    stream = stream_key(docker_host, Kind.LOG)
    needle = query.lower()

    def _hit(entry_id: bytes | str, fields: dict | None) -> str | None:
        record = _decode_entry(fields)
        if (
            record is not None
            and record.container_id == container_id
            and needle in str(getattr(record, "message", "")).lower()
        ):
            return _decode(entry_id)
        return None

    async def _scan_forward(lo: str, hi: str) -> str | None:
        scanned = 0
        while scanned < _FIND_MAX_SCANNED:
            page = await redis.xrange(stream, min=lo, max=hi, count=_FIND_PAGE) or []
            if not page:
                return None
            for entry_id, fields in page:
                scanned += 1
                if entry_id is None:
                    continue
                if (hit := _hit(entry_id, fields)) is not None:
                    return hit
            last_id = page[-1][0]
            assert last_id is not None  # page was non-empty, so its own last entry has an id
            lo = f"({_decode(last_id)}"
        return None

    async def _scan_backward(lo: str, hi: str) -> str | None:
        scanned = 0
        while scanned < _FIND_MAX_SCANNED:
            page = await redis.xrevrange(stream, max=hi, min=lo, count=_FIND_PAGE) or []
            if not page:
                return None
            for entry_id, fields in page:
                scanned += 1
                if entry_id is None:
                    continue
                if (hit := _hit(entry_id, fields)) is not None:
                    return hit
            last_id = page[-1][0]
            assert last_id is not None  # page was non-empty, so its own last entry has an id
            hi = f"({_decode(last_id)}"
        return None

    if forward:
        start = f"({cursor}" if cursor else "-"
        hit = await _scan_forward(start, "+")
        if hit is not None or not cursor:
            return hit
        return await _scan_forward("-", f"({cursor}")  # wrap to the beginning

    start = f"({cursor}" if cursor else "+"
    hit = await _scan_backward("-", start)
    if hit is not None or not cursor:
        return hit
    return await _scan_backward(f"({cursor}", "+")  # wrap to the end


def apply_filters(
    records: list[LogRecord | MetricRecord],
    *,
    container_id: str | None,
    level: str | None,
    q: str | None,
) -> list[LogRecord | MetricRecord]:
    """Post-`XRANGE` filters that aren't expressible as a Stream ID range."""
    result = records
    if container_id is not None:
        result = [r for r in result if r.container_id == container_id]
    if level is not None:
        # getattr rather than `isinstance(r, LogRecord)` -- MetricRecord has
        # no `level`, so it never matches and stays filtered out, without
        # narrowing the list's element type down to just LogRecord.
        result = [r for r in result if getattr(r, "level", None) == level]
    if q is not None:
        needle = q.lower()
        result = [r for r in result if needle in str(getattr(r, "message", "")).lower()]
    return result
