"""POST /session/start, POST /session/{id}/stop, POST /session/{id}/safe,
GET /session/{id}/status, GET /session/{id}/download, POST /session/ttl --
migration plan Phase 4. See sessions.py for the actual bookkeeping.

Auth: starting a session against a `docker_host` requires access to that
daemon, same as any other route (`require_daemon_access`). Every other
action here (stop/safe/status/download) takes just a session id, with no
`docker_host` of its own to check -- gated by `require_valid_api_key`
(any known key) instead, matching cttc's own model, which has no
per-resource authorization at all beyond its single shared gateway token.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from ..deps import (
    get_permitted_daemons,
    get_session_manager,
    require_daemon_access,
    require_valid_api_key,
)
from ..sessions import SessionManager, UnknownSession

router = APIRouter()


class SessionStartRequest(BaseModel):
    docker_host: str
    duration_minutes: float | None = None
    safe: bool = False
    max_keep_seconds: float | None = None


class SessionStartResponse(BaseModel):
    session_id: str


@router.post("/session/start")
async def start_session(
    body: SessionStartRequest,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    sessions: Annotated[SessionManager, Depends(get_session_manager)],
) -> SessionStartResponse:
    require_daemon_access(body.docker_host, permitted)
    session_id = sessions.start(
        body.docker_host,
        duration_minutes=body.duration_minutes,
        safe=body.safe,
        max_keep_seconds=body.max_keep_seconds,
    )
    return SessionStartResponse(session_id=session_id)


class OkResponse(BaseModel):
    ok: bool = True


@router.post("/session/{session_id}/stop", dependencies=[Depends(require_valid_api_key)])
async def stop_session(
    session_id: str, sessions: Annotated[SessionManager, Depends(get_session_manager)]
) -> OkResponse:
    try:
        await sessions.stop(session_id)
    except UnknownSession as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown session: {session_id}") from exc
    return OkResponse()


class SessionSafeRequest(BaseModel):
    max_keep_seconds: float


@router.post("/session/{session_id}/safe", dependencies=[Depends(require_valid_api_key)])
async def mark_session_safe(
    session_id: str,
    body: SessionSafeRequest,
    sessions: Annotated[SessionManager, Depends(get_session_manager)],
) -> OkResponse:
    try:
        sessions.mark_safe(session_id, body.max_keep_seconds)
    except UnknownSession as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown session: {session_id}") from exc
    return OkResponse()


@router.get("/session/{session_id}/status", dependencies=[Depends(require_valid_api_key)])
async def session_status(
    session_id: str, sessions: Annotated[SessionManager, Depends(get_session_manager)]
) -> dict:
    try:
        return sessions.status_of(session_id)
    except UnknownSession as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown session: {session_id}") from exc


@router.get("/session/{session_id}/download", dependencies=[Depends(require_valid_api_key)])
async def download_session(
    session_id: str, sessions: Annotated[SessionManager, Depends(get_session_manager)]
) -> Response:
    try:
        data = await sessions.download(session_id)
    except UnknownSession as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"unknown or not-yet-completed session: {session_id}"
        ) from exc
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{session_id}.cttc-record"'},
    )


class SessionTtlRequest(BaseModel):
    seconds: float


@router.post("/session/ttl", dependencies=[Depends(require_valid_api_key)])
async def set_session_ttl(
    body: SessionTtlRequest, sessions: Annotated[SessionManager, Depends(get_session_manager)]
) -> OkResponse:
    sessions.set_default_ttl(body.seconds)
    return OkResponse()
