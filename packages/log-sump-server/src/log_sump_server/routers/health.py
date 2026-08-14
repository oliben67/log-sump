"""Liveness/readiness (spec §10). No auth: these are hit by infrastructure
(load balancers, container orchestrators), not API clients.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ..deps import get_redis

router = APIRouter()


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(
    redis: Annotated[Redis, Depends(get_redis)], response: Response
) -> dict[str, str]:
    try:
        await redis.ping()
    except RedisError:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "redis unreachable"}
    return {"status": "ok"}
