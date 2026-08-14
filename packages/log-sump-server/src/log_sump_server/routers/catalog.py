"""GET /catalog (spec §9): the daemons this log-sump instance watches,
filtered to the ones the authenticated caller is permitted to see. The
static YAML-configured list, merged with anything registered at runtime
since (migration plan Phase 3, `POST /daemons` -- see
`log_sump_common.daemon_registry`); §10's "surface per-daemon reachability
status" is still a documented seam for a future addition, not built in
this pass.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from log_sump_common.config import Settings
from log_sump_common.daemon_registry import list_registered_daemons
from pydantic import BaseModel
from redis.asyncio import Redis

from ..deps import get_permitted_daemons, get_redis, get_settings

router = APIRouter()


class CatalogEntry(BaseModel):
    id: str
    host: str
    enabled: bool


@router.get("/catalog")
async def get_catalog(
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    settings: Annotated[Settings, Depends(get_settings)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> list[CatalogEntry]:
    by_id = {daemon.id: daemon for daemon in settings.daemons}
    for daemon in await list_registered_daemons(redis):
        by_id.setdefault(daemon.id, daemon)  # YAML wins on a (rare) id collision
    return [
        CatalogEntry(id=daemon.id, host=daemon.host, enabled=daemon.enabled)
        for daemon in by_id.values()
        if daemon.id in permitted
    ]
