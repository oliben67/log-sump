"""Read-only Redis inspection endpoint — see `redis_inspect.py` for the
command allowlist and the reasoning behind this endpoint's scope.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ..deps import get_redis, require_valid_api_key
from ..redis_inspect import CommandNotAllowed, check_command_allowed

router = APIRouter()


class RedisCommandRequest(BaseModel):
    command: str
    args: list[str] = []


class RedisCommandResponse(BaseModel):
    result: Any


@router.post(
    "/admin/redis/command",
    dependencies=[Depends(require_valid_api_key)],
)
async def run_redis_command(
    body: RedisCommandRequest, redis: Annotated[Redis, Depends(get_redis)]
) -> RedisCommandResponse:
    try:
        command = check_command_allowed(body.command)
    except CommandNotAllowed as exc:
        raise HTTPException(status.HTTP_403_FORBIDDEN, str(exc)) from exc

    try:
        result = await redis.execute_command(command, *body.args)
    except RedisError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    return RedisCommandResponse(result=_decode(result))


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_decode(v) for v in value)
    if isinstance(value, dict):
        return {_decode(k): _decode(v) for k, v in value.items()}
    return value
