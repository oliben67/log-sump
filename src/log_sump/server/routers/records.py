"""GET /records (spec §5.5): logs and/or metrics for one daemon (optionally
one container), over a time range, kind-filtered, paginated, and — when
both kinds are requested — interleaved by `ts` onto one contiguous
timeline. Also covers fetching daemon-level (`__system__`) metrics, since
those are just regular metric records with `container_id = "__system__"`
(spec §6) — pass `container_id=__system__` like any other filter.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from redis.asyncio import Redis

from log_sump.common.config import Settings
from log_sump.common.schema import Kind, LogRecord, MetricRecord, Record

from ..deps import get_permitted_daemons, get_redis, get_settings, require_daemon_access
from ..queries import apply_filters, fetch_kind_page

router = APIRouter()


class RecordsPage(BaseModel):
    records: list[Record]
    next_log_cursor: str | None = None
    next_metric_cursor: str | None = None


@router.get("/records")
async def get_records(
    docker_host: str,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
    settings: Annotated[Settings, Depends(get_settings)],
    kind: Annotated[str, Query(pattern="^(log|metric|both)$")] = "both",
    container_id: str | None = None,
    level: str | None = None,
    q: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    log_cursor: str | None = None,
    metric_cursor: str | None = None,
    limit: int | None = None,
) -> RecordsPage:
    require_daemon_access(docker_host, permitted)

    effective_limit = min(limit or settings.server.page_size_default, settings.server.page_size_max)
    now = datetime.now(UTC)
    range_start = start or (now - timedelta(hours=1))
    range_end = end or now
    kinds = [Kind.LOG, Kind.METRIC] if kind == "both" else [Kind(kind)]

    all_records: list[LogRecord | MetricRecord] = []
    next_log_cursor: str | None = None
    next_metric_cursor: str | None = None

    if Kind.LOG in kinds:
        entries, next_log_cursor = await fetch_kind_page(
            redis,
            docker_host,
            Kind.LOG,
            start=range_start,
            end=range_end,
            cursor=log_cursor,
            limit=effective_limit,
        )
        all_records.extend(record for _id, record in entries)
    if Kind.METRIC in kinds:
        entries, next_metric_cursor = await fetch_kind_page(
            redis,
            docker_host,
            Kind.METRIC,
            start=range_start,
            end=range_end,
            cursor=metric_cursor,
            limit=effective_limit,
        )
        all_records.extend(record for _id, record in entries)

    all_records.sort(key=lambda r: r.ts)
    filtered = apply_filters(all_records, container_id=container_id, level=level, q=q)

    return RecordsPage(
        records=filtered, next_log_cursor=next_log_cursor, next_metric_cursor=next_metric_cursor
    )
