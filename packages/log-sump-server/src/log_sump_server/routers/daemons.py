"""POST /daemons, DELETE /daemons/{id}: runtime daemon registration
(migration plan Phase 3) -- closes the `/docker/collect`/`/docker/forget`/
"Set Sources" gap. Writes to the same Redis hash log-listener's
`DaemonManager` polls (`log_sump_common.daemon_registry`); DELETE only ever
succeeds for a daemon that was itself registered through this endpoint --
a YAML-configured daemon can't be removed this way (see
`run_daemon_registry_watch`'s own "never touches a YAML-seeded daemon"
rule on the listener side).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from log_sump_common.config import DaemonConfig
from log_sump_common.daemon_registry import register_daemon, unregister_daemon
from log_sump_common.redis_keys import auth_key
from redis.asyncio import Redis

from ..deps import get_raw_api_key, get_redis
from .catalog import CatalogEntry

router = APIRouter()


@router.post("/daemons")
async def create_daemon(
    daemon: DaemonConfig,
    redis: Annotated[Redis, Depends(get_redis)],
    api_key: Annotated[str, Depends(get_raw_api_key)],
) -> CatalogEntry:
    await register_daemon(redis, daemon)
    # Same auto-provisioning as file upload (local_upload.py): the caller
    # that just registered a daemon can immediately see/query it, no
    # separate grant step.
    await redis.sadd(auth_key(api_key), daemon.id)
    return CatalogEntry(id=daemon.id, host=daemon.host, enabled=daemon.enabled)


@router.delete("/daemons/{daemon_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_daemon(
    daemon_id: str,
    redis: Annotated[Redis, Depends(get_redis)],
    _api_key: Annotated[str, Depends(get_raw_api_key)],
) -> None:
    removed = await unregister_daemon(redis, daemon_id)
    if not removed:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"{daemon_id!r} is not a dynamically-registered daemon "
            "(a YAML-configured daemon can't be removed through this endpoint)",
        )
