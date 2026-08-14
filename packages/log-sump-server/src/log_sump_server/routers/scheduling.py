"""POST /scheduler/create, GET /scheduler/{id}, POST /scheduler/{id}/cancel
-- migration plan Phase 4. See scheduling.py for the actual bookkeeping.
Same auth split as routers/sessions.py: creating a schedule against a
`docker_host` requires access to that daemon; status/cancel on an existing
schedule id just requires any known key.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ..deps import (
    get_permitted_daemons,
    get_scheduler,
    require_daemon_access,
    require_valid_api_key,
)
from ..scheduling import InvalidSchedule, Scheduler, UnknownSchedule

router = APIRouter()


class ScheduleCreateRequest(BaseModel):
    docker_host: str
    duration_minutes: float
    start_at: float | None = None
    cron: str | None = None
    safe: bool = False
    max_keep_seconds: float | None = None


class ScheduleCreateResponse(BaseModel):
    schedule_id: str


@router.post("/scheduler/create")
async def create_schedule(
    body: ScheduleCreateRequest,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    scheduler: Annotated[Scheduler, Depends(get_scheduler)],
) -> ScheduleCreateResponse:
    require_daemon_access(body.docker_host, permitted)
    try:
        schedule_id = scheduler.create(
            body.docker_host,
            body.duration_minutes,
            start_at=body.start_at,
            cron=body.cron,
            safe=body.safe,
            max_keep_seconds=body.max_keep_seconds,
        )
    except InvalidSchedule as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return ScheduleCreateResponse(schedule_id=schedule_id)


@router.get("/scheduler/{schedule_id}", dependencies=[Depends(require_valid_api_key)])
async def schedule_status(
    schedule_id: str, scheduler: Annotated[Scheduler, Depends(get_scheduler)]
) -> dict:
    try:
        return scheduler.status_of(schedule_id)
    except UnknownSchedule as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown schedule: {schedule_id}") from exc


class OkResponse(BaseModel):
    ok: bool = True


@router.post("/scheduler/{schedule_id}/cancel", dependencies=[Depends(require_valid_api_key)])
async def cancel_schedule(
    schedule_id: str, scheduler: Annotated[Scheduler, Depends(get_scheduler)]
) -> OkResponse:
    try:
        scheduler.cancel(schedule_id)
    except UnknownSchedule as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown schedule: {schedule_id}") from exc
    return OkResponse()
