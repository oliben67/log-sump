"""POST /files/upload: import a local log file or .cttc-metric/.cttc-record
archive with no daemon/container involved at all (migration plan Phase 2).
See local_upload.py for the actual parsing/ingestion.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from log_sump_common.cttc_archive import MultiSegmentArchive
from pydantic import BaseModel
from redis.asyncio import Redis

from ..broadcast import Broadcaster
from ..deps import get_broadcaster, get_raw_api_key, get_redis
from ..local_upload import ingest_upload

router = APIRouter()


class UploadResponse(BaseModel):
    docker_host: str
    log_count: int
    metric_count: int


@router.post("/files/upload")
async def upload_file(
    file: UploadFile,
    redis: Annotated[Redis, Depends(get_redis)],
    api_key: Annotated[str, Depends(get_raw_api_key)],
    broadcaster: Annotated[Broadcaster, Depends(get_broadcaster)],
    segment: int | None = None,
) -> UploadResponse:
    data = await file.read()
    try:
        result = await ingest_upload(
            redis,
            filename=file.filename or "upload",
            data=data,
            api_key=api_key,
            segment=segment,
            broadcaster=broadcaster,
        )
    except MultiSegmentArchive as exc:
        detail = {
            "error": "archive holds multiple segments -- pick one via ?segment=",
            "segments": exc.segments,
        }
        raise HTTPException(status.HTTP_409_CONFLICT, detail) from exc
    return UploadResponse(
        docker_host=result.docker_host,
        log_count=result.log_count,
        metric_count=result.metric_count,
    )
