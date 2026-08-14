"""GET /catalog (spec §9): the daemons this log-sump instance watches,
filtered to the ones the authenticated caller is permitted to see. The
catalog itself is just the static configured daemon list — no live
container inventory here (that's the registry's job, in-process inside
log-listener); §10's "surface per-daemon reachability status" is a
documented seam for a future addition, not built in this pass.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from log_sump_common.config import Settings
from pydantic import BaseModel

from ..deps import get_permitted_daemons, get_settings

router = APIRouter()


class CatalogEntry(BaseModel):
    id: str
    host: str
    enabled: bool


@router.get("/catalog")
async def get_catalog(
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> list[CatalogEntry]:
    return [
        CatalogEntry(id=daemon.id, host=daemon.host, enabled=daemon.enabled)
        for daemon in settings.daemons
        if daemon.id in permitted
    ]
