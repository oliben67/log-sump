"""Timeline queries the cttc scrubbing UI needs on top of log-sump's
existing per-daemon Streams (migration plan, Phase 1): `/point`, `/ticks`,
`/series`, `/logs/find`, `/index_at`. Daemon-scoped, same auth dependency as
`/records` -- see `queries.py` for the actual `XRANGE` logic each of these
wraps.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from redis.asyncio import Redis

from log_sump.common.schema import Kind, MetricRecord

from ..deps import get_permitted_daemons, get_redis, require_daemon_access
from ..queries import bucketed, find_text, index_at, latest_services, point_at, ticks

router = APIRouter()


class PointService(BaseModel):
    container_id: str
    container_name: str
    ttype: Literal["service", "container"]
    ts: datetime
    cpu_pct: float | None = None
    mem_pct: float | None = None
    mem_used_bytes: int | None = None


class PointResponse(BaseModel):
    t: datetime
    services: dict[str, PointService]


@router.get("/point")
async def get_point(
    docker_host: str,
    t: datetime,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> PointResponse:
    require_daemon_access(docker_host, permitted)
    nearest = await point_at(redis, docker_host, Kind.METRIC, t)
    services = {
        group: PointService(
            container_id=record.container_id,
            container_name=record.container_name,
            ttype="service" if is_service else "container",
            ts=record.ts,
            cpu_pct=record.cpu_pct,
            mem_pct=record.mem_pct,
            mem_used_bytes=record.mem_used_bytes,
        )
        for group, (_entry_id, record, is_service) in nearest.items()
        if isinstance(record, MetricRecord)  # point_at is generic over Kind; narrow for mypy/ty
    }
    return PointResponse(t=t, services=services)


class IndexAtResponse(BaseModel):
    cursor: str | None


@router.get("/index_at")
async def get_index_at(
    docker_host: str,
    container_id: str,
    t: datetime,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> IndexAtResponse:
    require_daemon_access(docker_host, permitted)
    return IndexAtResponse(cursor=await index_at(redis, docker_host, container_id, t))


class TicksResponse(BaseModel):
    counts: list[int]


@router.get("/ticks")
async def get_ticks(
    docker_host: str,
    container_id: str,
    start: datetime,
    end: datetime,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
    kind: Annotated[str, Query(pattern="^(log|metric)$")] = "log",
    px: int = 800,
) -> TicksResponse:
    require_daemon_access(docker_host, permitted)
    counts = await ticks(redis, docker_host, container_id, Kind(kind), start, end, px)
    return TicksResponse(counts=counts)


class SeriesEntry(BaseModel):
    name: str
    ttype: Literal["service", "container"]
    cpu: list[float | None]
    mem: list[float | None]
    net: list[float | None]


class SeriesResponse(BaseModel):
    start: datetime
    end: datetime
    px: int
    containers: list[SeriesEntry]


@router.get("/series")
async def get_series(
    docker_host: str,
    start: datetime,
    end: datetime,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
    px: int = 800,
) -> SeriesResponse:
    require_daemon_access(docker_host, permitted)
    containers = await bucketed(redis, docker_host, start, end, px)
    return SeriesResponse(
        start=start, end=end, px=px, containers=[SeriesEntry(**c) for c in containers]
    )


class FindResponse(BaseModel):
    cursor: str | None


@router.get("/logs/find")
async def get_logs_find(
    docker_host: str,
    container_id: str,
    q: str,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
    cursor: str | None = None,
    direction: Annotated[str, Query(pattern="^(fwd|back)$")] = "fwd",
) -> FindResponse:
    require_daemon_access(docker_host, permitted)
    hit = await find_text(
        redis, docker_host, container_id, q, cursor=cursor, forward=direction != "back"
    )
    return FindResponse(cursor=hit)


class ServiceEntry(BaseModel):
    id: str
    name: str
    replicas: str


class ServicesResponse(BaseModel):
    services: list[ServiceEntry]


@router.get("/services")
async def get_services(
    docker_host: str,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> ServicesResponse:
    """The daemon's currently-listed swarm services (`docker service ls`) --
    cttc's `docker_ps`'s "services" list, offered by the Set Sources picker
    alongside individual containers. Empty on a non-swarm daemon, not an
    error (see `services_listing.py`'s own tolerance for "not a manager").
    """
    require_daemon_access(docker_host, permitted)
    records = await latest_services(redis, docker_host)
    return ServicesResponse(
        services=[ServiceEntry(id=r.id, name=r.name, replicas=r.replicas) for r in records]
    )
