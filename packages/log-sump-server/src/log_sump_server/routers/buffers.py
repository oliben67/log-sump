"""POST /buffer/start, POST /buffer/{id}/pause, POST /buffer/{id}/stop --
migration plan Phase 4. See buffers.py for the actual bookkeeping. Same
auth split as routers/sessions.py: starting a buffer against a
`docker_host` requires access to that daemon; pause/stop on an existing
buffer id just requires any known key.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from ..buffers import BufferManager, TooManyBuffers, UnknownBuffer
from ..deps import (
    get_buffer_manager,
    get_permitted_daemons,
    require_daemon_access,
    require_valid_api_key,
)

router = APIRouter()


class BufferStartRequest(BaseModel):
    docker_host: str
    minutes: float


class BufferStartResponse(BaseModel):
    buffer_id: str


@router.post("/buffer/start")
async def start_buffer(
    body: BufferStartRequest,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    buffers: Annotated[BufferManager, Depends(get_buffer_manager)],
) -> BufferStartResponse:
    require_daemon_access(body.docker_host, permitted)
    try:
        buffer_id = buffers.start(body.docker_host, body.minutes)
    except TooManyBuffers as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return BufferStartResponse(buffer_id=buffer_id)


class OkResponse(BaseModel):
    ok: bool = True


@router.post("/buffer/{buffer_id}/pause", dependencies=[Depends(require_valid_api_key)])
async def pause_buffer(
    buffer_id: str, buffers: Annotated[BufferManager, Depends(get_buffer_manager)]
) -> OkResponse:
    try:
        buffers.pause(buffer_id)
    except UnknownBuffer as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown buffer: {buffer_id}") from exc
    return OkResponse()


@router.post("/buffer/{buffer_id}/stop", dependencies=[Depends(require_valid_api_key)])
async def stop_buffer(
    buffer_id: str, buffers: Annotated[BufferManager, Depends(get_buffer_manager)]
) -> Response:
    try:
        data = await buffers.stop(buffer_id)
    except UnknownBuffer as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown buffer: {buffer_id}") from exc
    return Response(content=data, media_type="application/octet-stream")
