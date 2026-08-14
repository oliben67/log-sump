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
        if entry_id is None or fields is None:
            continue
        data = fields.get(b"data")
        if data is None:
            continue
        try:
            record = RecordAdapter.validate_json(data)
        except ValidationError:
            continue  # already validated once by the consumer; be lenient on read
        entries.append((_decode(entry_id), record))

    next_cursor = entries[-1][0] if len(raw_entries) == limit else None
    return entries, next_cursor


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


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
