"""POST /events/create, GET /events/list, GET /events/{id},
POST /events/{id}/enable, /disable, /reset, /update, /cancel -- migration
plan Phase 5. See events.py for the actual watch/fire bookkeeping. Same
auth split as routers/sessions.py: creating an event against a
`docker_host` requires access to that daemon; every other action on an
existing event id just requires any known key.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ..deps import (
    get_event_manager,
    get_permitted_daemons,
    require_daemon_access,
    require_valid_api_key,
)
from ..events import (
    Action,
    EventManager,
    InvalidEvent,
    LogCondition,
    MetricCondition,
    UnknownEvent,
)

router = APIRouter()


class ConditionBody(BaseModel):
    type: Literal["metric", "log"]
    metric: Literal["cpu", "mem", "net"] | None = None
    op: Literal[">", "<", ">=", "<=", "="] | None = None
    threshold: float | None = None
    pattern: str | None = None


class ActionBody(BaseModel):
    kind: Literal["snapshot", "recording"]
    minutes: float | None = None
    duration_minutes: float | None = None
    safe: bool = False
    max_keep_seconds: float | None = None


def _parse_condition(body: ConditionBody) -> MetricCondition | LogCondition:
    if body.type == "metric":
        if body.metric is None or body.op is None or body.threshold is None:
            raise InvalidEvent("a metric condition needs metric, op, and threshold")
        return MetricCondition(metric=body.metric, op=body.op, threshold=body.threshold)
    if body.pattern is None:
        raise InvalidEvent("a log condition needs pattern")
    return LogCondition(pattern=body.pattern)


def _parse_action(body: ActionBody) -> Action:
    return Action(
        kind=body.kind,
        minutes=body.minutes,
        duration_minutes=body.duration_minutes,
        safe=body.safe,
        max_keep_seconds=body.max_keep_seconds,
    )


class EventCreateRequest(BaseModel):
    name: str = ""
    docker_host: str
    conditions: list[ConditionBody] = []
    action: ActionBody
    match: Literal["any", "all"] = "any"


class EventCreateResponse(BaseModel):
    event_id: str


@router.post("/events/create")
async def create_event(
    body: EventCreateRequest,
    permitted: Annotated[frozenset[str], Depends(get_permitted_daemons)],
    events: Annotated[EventManager, Depends(get_event_manager)],
) -> EventCreateResponse:
    require_daemon_access(body.docker_host, permitted)
    try:
        conditions = [_parse_condition(c) for c in body.conditions]
        action = _parse_action(body.action)
        event_id = await events.create(
            name=body.name,
            docker_host=body.docker_host,
            conditions=conditions,
            action=action,
            match=body.match,
        )
    except InvalidEvent as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return EventCreateResponse(event_id=event_id)


class EventListResponse(BaseModel):
    event_ids: list[str]


@router.get("/events/list", dependencies=[Depends(require_valid_api_key)])
async def list_events(
    events: Annotated[EventManager, Depends(get_event_manager)],
) -> EventListResponse:
    return EventListResponse(event_ids=events.list_ids())


@router.get("/events/{event_id}", dependencies=[Depends(require_valid_api_key)])
async def event_status(
    event_id: str, events: Annotated[EventManager, Depends(get_event_manager)]
) -> dict:
    try:
        return events.status_of(event_id)
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc


class OkResponse(BaseModel):
    ok: bool = True


@router.post("/events/{event_id}/enable", dependencies=[Depends(require_valid_api_key)])
async def enable_event(
    event_id: str, events: Annotated[EventManager, Depends(get_event_manager)]
) -> OkResponse:
    try:
        events.enable(event_id)
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc
    return OkResponse()


@router.post("/events/{event_id}/disable", dependencies=[Depends(require_valid_api_key)])
async def disable_event(
    event_id: str, events: Annotated[EventManager, Depends(get_event_manager)]
) -> OkResponse:
    try:
        events.disable(event_id)
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc
    return OkResponse()


@router.post("/events/{event_id}/reset", dependencies=[Depends(require_valid_api_key)])
async def reset_event(
    event_id: str, events: Annotated[EventManager, Depends(get_event_manager)]
) -> OkResponse:
    try:
        events.reset(event_id)
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc
    return OkResponse()


class EventUpdateRequest(BaseModel):
    name: str | None = None
    docker_host: str | None = None
    conditions: list[ConditionBody] | None = None
    action: ActionBody | None = None
    match: Literal["any", "all"] | None = None


@router.post("/events/{event_id}/update", dependencies=[Depends(require_valid_api_key)])
async def update_event(
    event_id: str,
    body: EventUpdateRequest,
    events: Annotated[EventManager, Depends(get_event_manager)],
) -> OkResponse:
    try:
        await events.update(
            event_id,
            name=body.name,
            docker_host=body.docker_host,
            conditions=[_parse_condition(c) for c in body.conditions]
            if body.conditions is not None
            else None,
            action=_parse_action(body.action) if body.action is not None else None,
            match=body.match,
        )
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc
    except InvalidEvent as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return OkResponse()


@router.post("/events/{event_id}/cancel", dependencies=[Depends(require_valid_api_key)])
async def cancel_event(
    event_id: str, events: Annotated[EventManager, Depends(get_event_manager)]
) -> OkResponse:
    try:
        await events.cancel(event_id)
    except UnknownEvent as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown event: {event_id}") from exc
    return OkResponse()
