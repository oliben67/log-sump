"""GET /transforms: list the available user transform plugins (migration
plan Phase 5). See transforms.py for the actual plugin loading.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ..deps import require_valid_api_key
from ..transforms import TransformRegistry

router = APIRouter()


class TransformInfo(BaseModel):
    name: str
    doc: str


class TransformsResponse(BaseModel):
    transforms: list[TransformInfo]


@router.get("/transforms", dependencies=[Depends(require_valid_api_key)])
async def list_transforms(request: Request) -> TransformsResponse:
    registry: TransformRegistry | None = request.app.state.transform_registry
    if registry is None:
        return TransformsResponse(transforms=[])
    return TransformsResponse(transforms=[TransformInfo(**t) for t in registry.available()])
