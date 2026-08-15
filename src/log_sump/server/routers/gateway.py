"""Gateway-mesh + admin-auth routes (migration plan Phase 7): `/ping`,
`/gateways/sync`, gateway ownership claim/challenge/rotate, and the small
dev-tool routes cttc groups alongside them (`/mlog`, `/admin/redis-cli`,
`/shutdown`) -- ported from `server.py`'s equivalents with no behavior
change intended (see `gateway_mesh.py`'s own module docstring). Every route
here is gated by the shared gateway token (`deps.require_gateway_token`),
except `/ping` -- cttc's own `_UNAUTHENTICATED_PATHS` exemption, since a
client that just learned about a peer via mesh sync has no token for it
yet.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from log_sump.common.config import Settings

from .. import gateway_mesh
from ..deps import get_redis, get_settings, require_gateway_token

router = APIRouter()
#: Applied explicitly to every route below except `/ping` (see module
#: docstring) -- an `APIRouter(dependencies=...)` constructor-level gate
#: was tried first, but it also reaches routes added through a nested
#: `include_router()` call (confirmed empirically), so there's no way to
#: carve out one exemption that way; per-route `dependencies=` is the only
#: option that actually leaves `/ping` open.
_gated = [Depends(require_gateway_token)]


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@router.get("/ping")
async def route_ping() -> dict[str, str]:
    """br-MESH-006: unauthenticated L7 liveness that identifies this as
    specifically a gateway, unlike `/health/*` -- lets a client tell "a
    gateway answered" from "some port is open" for a peer discovered via
    mesh sync it has no token for yet.
    """
    return {"service": "gateway", "version": gateway_mesh.GATEWAY_VERSION}


class GatewaysSyncRequest(BaseModel):
    entries: list[dict[str, Any]]


class GatewaysSyncResponse(BaseModel):
    entries: list[dict[str, Any]]


def _self_address(request: Request, settings: Settings) -> str:
    """This gateway's own host:port, as it should appear in its peer list
    (br-MESH-002). `settings.gateway.public_address` wins if set -- the
    operator knows this gateway's real externally-reachable address better
    than anything inferred -- else the incoming request's own `Host`
    header, which reflects whatever address the client actually dialed to
    reach us.
    """
    return settings.gateway.public_address or request.headers.get("host", "")


@router.post("/gateways/sync", dependencies=_gated)
async def route_gateways_sync(
    body: GatewaysSyncRequest,
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> GatewaysSyncResponse:
    """br-MESH-003/004/005: merges the client's posted gateway list into
    this gateway's own persisted one under trust rules that stop relayed/
    stale belief from overwriting something directly verified, then returns
    the full merged list. The self-entry is (re)written last and
    unconditionally, so it's always authoritative for itself regardless of
    anything the client happened to relay about this same address.
    """
    current = await gateway_mesh.load_gateway_list(redis)
    max_entries = settings.gateway.gateway_list_max_entries
    # br-MESH-005: bound accepted list size -- trim the posted payload
    # itself rather than let a pathologically large one grow the merge (and
    # the persisted list) without limit.
    for incoming in body.entries[:max_entries]:
        host, port = incoming.get("host"), incoming.get("port")
        if not host or not port:
            continue
        key = gateway_mesh.canonical_gateway_key(host, port)
        current[key] = gateway_mesh.merge_gateway_entry(current.get(key), incoming)
    self_addr = _self_address(request, settings)
    self_host, self_port = gateway_mesh.split_host_port(self_addr)
    self_key = gateway_mesh.canonical_gateway_key(self_host, self_port)
    current[self_key] = gateway_mesh.self_gateway_entry(self_addr, now_iso=_now_iso())
    if len(current) > max_entries:
        # Trim oldest-by-lastContactAt, but the self-entry is never the one
        # to go -- re-added unconditionally after the trim if the cut
        # happened to exclude it.
        kept = sorted(
            current.items(), key=lambda kv: kv[1].get("lastContactAt") or "", reverse=True
        )
        current = dict(kept[:max_entries])
        current[self_key] = gateway_mesh.self_gateway_entry(self_addr, now_iso=_now_iso())
    await gateway_mesh.save_gateway_list(redis, current)
    return GatewaysSyncResponse(entries=list(current.values()))


class OwnershipClaimRequest(BaseModel):
    ownerLabel: str = Field(min_length=1)
    ownerPublicKey: str = Field(min_length=1)


class OwnershipRecord(BaseModel):
    ownerLabel: str
    ownerPublicKey: str
    ownerKeyFingerprint: str
    installedAt: str
    updatedAt: str


@router.post("/gateway/ownership/claim", dependencies=_gated)
async def route_gateway_ownership_claim(
    body: OwnershipClaimRequest, redis: Annotated[Redis, Depends(get_redis)]
) -> OwnershipRecord:
    """br-OWNER-001 (REQ-0069): the deploying client becomes owner. Called
    once, right after provisioning's health check succeeds. Idempotent by
    design -- if an ownership record already exists, this is a no-op that
    returns it unchanged: connecting to an already-owned gateway must never
    rewrite who owns it. The read here is just a fast path;
    `write_ownership`'s SET NX is what actually makes the write atomic, so
    two clients racing to claim the same fresh gateway can never both "win"
    (the loser reads back the winner's record below).
    """
    existing = await gateway_mesh.read_ownership(redis)
    if existing is not None:
        return OwnershipRecord(**existing)
    now = _now_iso()
    record = {
        "ownerLabel": body.ownerLabel,
        "ownerPublicKey": body.ownerPublicKey,
        "ownerKeyFingerprint": gateway_mesh.ownership_fingerprint(body.ownerPublicKey),
        "installedAt": now,
        "updatedAt": now,
    }
    if not await gateway_mesh.write_ownership(redis, record):
        existing = await gateway_mesh.read_ownership(redis)
        assert existing is not None
        return OwnershipRecord(**existing)
    return OwnershipRecord(**record)


class AdminChallengeResponse(BaseModel):
    nonce: str
    expiresInSeconds: float


@router.get("/gateway/admin/challenge", dependencies=_gated)
async def route_gateway_admin_challenge(
    redis: Annotated[Redis, Depends(get_redis)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> AdminChallengeResponse:
    """br-OWNER-003 (REQ-0069): issues a short-lived, single-use nonce for
    the caller to sign and present back to an admin-tier route (currently
    just `/gateway/ownership/rotate`). A nonce on its own authorizes
    nothing; only a *signature* over it, verified against the current
    owner's public key, does.
    """
    nonce = uuid4().hex
    ttl = settings.gateway.admin_nonce_ttl_seconds
    await gateway_mesh.remember_nonce(redis, nonce, ttl)
    return AdminChallengeResponse(nonce=nonce, expiresInSeconds=ttl)


class OwnershipRotateRequest(BaseModel):
    newOwnerLabel: str = Field(min_length=1)
    newOwnerPublicKey: str = Field(min_length=1)
    nonce: str
    signature: str


@router.post("/gateway/ownership/rotate", dependencies=_gated)
async def route_gateway_ownership_rotate(
    body: OwnershipRotateRequest, request: Request, redis: Annotated[Redis, Depends(get_redis)]
) -> OwnershipRecord:
    """br-OWNER-003 (REQ-0069): transfers ownership to a new owner -- the
    one ownership-record write path allowed to *replace* an existing
    record, gated on proof the *current* owner authorized it. Does not
    rotate the gateway token itself (a later phase's client reconnect logic
    would silently undo an in-memory-only rotation here, same reasoning as
    cttc's own open question on this point).
    """
    client_host = request.client.host if request.client else "?"
    current = await gateway_mesh.require_owner_signature(
        redis, body.model_dump(), "ownership.rotate", client_host=client_host
    )
    record = {
        "ownerLabel": body.newOwnerLabel,
        "ownerPublicKey": body.newOwnerPublicKey,
        "ownerKeyFingerprint": gateway_mesh.ownership_fingerprint(body.newOwnerPublicKey),
        "installedAt": current.get("installedAt", _now_iso()),
        "updatedAt": _now_iso(),
    }
    await gateway_mesh.overwrite_ownership(redis, record)
    return OwnershipRecord(**record)


@router.get("/mlog", dependencies=_gated)
async def route_mlog() -> Response:
    """Ship Logs (Settings > Collect CTTC Own Logs): `docker logs` on the
    gateway's own container, named after it. The filename travels in a
    header since a plain download response has no other structured place
    to carry it.
    """
    name, data = await gateway_mesh.gather_own_container_logs()
    return Response(
        content=data,
        media_type="text/plain",
        headers={
            "X-CTTC-Gateway-Name": name,
            "Content-Disposition": f'attachment; filename="{name}.log"',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "X-CTTC-Gateway-Name",
        },
    )


class RedisCliRequest(BaseModel):
    argv: list[str] = Field(min_length=1)


@router.post("/admin/redis-cli", dependencies=_gated)
async def route_admin_redis_cli(
    body: RedisCliRequest, redis: Annotated[Redis, Depends(get_redis)]
) -> dict[str, Any]:
    """Developer-only Redis CLI (Help > Developers menu) -- runs a raw
    command against this gateway's own internal Redis and returns a
    type-tagged reply the renderer formats in real-`redis-cli` style.
    Intentionally unrestricted (no blocked commands, unlike the existing
    read-only-allowlisted `/admin/redis/command`) -- dev-only tool, gated
    by the gateway token rather than a data-scoped API key.
    """
    raw_view = gateway_mesh.build_raw_redis_view(redis)
    try:
        raw = await gateway_mesh.execute_raw(raw_view, *body.argv)
    except Exception as e:  # noqa: BLE001 -- passed back as a typed CLI reply, not raised
        return {"type": "error", "value": str(e)}
    return gateway_mesh.redis_type_reply(raw)


@router.post("/shutdown", dependencies=_gated)
async def route_shutdown(request: Request) -> dict[str, bool]:
    server = request.app.state.uvicorn_server

    async def _stop() -> None:
        await asyncio.sleep(0.1)
        server.should_exit = True

    asyncio.ensure_future(_stop())
    return {"ok": True}
