"""Gateway ownership + peer-discovery mesh (migration plan Phase 7) --
ported from a prior gateway implementation's own `redis_log.py`
ownership/nonce/gateway-list methods and `server.py`'s mesh section, kept
in one module the way that implementation kept them in one file: this is
all one feature (br-OWNER-00x/br-MESH-00x), not several.

Ownership is the trust anchor every admin-tier action checks against
(`require_owner_signature`, used by `routers.gateway`'s
`/gateway/ownership/rotate`); the peer-discovery list (`load_gateway_list`/
`save_gateway_list`/`merge_gateway_entry`) is a separate, lower-stakes
concern that happens to live in the same Redis-backed "gateway identity"
namespace.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import json
from typing import Any

import structlog
from fastapi import HTTPException, status
from redis.asyncio import Redis

from log_sump.common.auth import verify_owner_signature
from log_sump.common.redis_keys import gateway_list_key, gateway_nonce_key, gateway_ownership_key

logger = structlog.get_logger(__name__)

try:
    GATEWAY_VERSION = importlib.metadata.version("log-sump")
except importlib.metadata.PackageNotFoundError:
    # Not installed as a package in every deployment mode -- /ping still
    # needs to answer with something rather than raise.
    GATEWAY_VERSION = "0.0.0-dev"


# ── gateway ownership (br-OWNER-001, REQ-0069) ──────────────────────────────


async def write_ownership(redis: Redis, record: dict[str, Any]) -> bool:
    """Writes the ownership record only if it doesn't exist yet (SET NX) --
    first claim wins, atomically, so a second client connecting to an
    already-owned gateway can never race a rewrite of the record
    (br-OWNER-001). No TTL: ownership must survive indefinitely, not decay
    like telemetry. Returns whether this call actually wrote the record
    (`False` means one already existed and was left untouched).
    """
    wrote = await redis.set(gateway_ownership_key(), json.dumps(record), nx=True)
    return bool(wrote)


async def read_ownership(redis: Redis) -> dict[str, Any] | None:
    raw = await redis.get(gateway_ownership_key())
    return json.loads(raw) if raw is not None else None


async def overwrite_ownership(redis: Redis, record: dict[str, Any]) -> None:
    """Unconditional SET, unlike `write_ownership`'s SET NX -- the one path
    allowed to *replace* an existing ownership record rather than only ever
    establish a first one. Callers (`routers.gateway`'s
    `/gateway/ownership/rotate`) are responsible for the actual
    authorization check (`require_owner_signature`) before ever reaching
    this; this function itself trusts its caller completely.
    """
    await redis.set(gateway_ownership_key(), json.dumps(record))


# ── admin-action nonces (br-OWNER-003, REQ-0069) ────────────────────────────


async def remember_nonce(redis: Redis, nonce: str, ttl_seconds: float) -> None:
    """Short-lived, single-use challenge for admin-action authorization
    (br-OWNER-003) -- SETEX so an unconsumed nonce expires on its own
    rather than accumulating forever.
    """
    if not nonce:
        return
    await redis.set(gateway_nonce_key(nonce), "1", ex=int(ttl_seconds))


async def consume_nonce(redis: Redis, nonce: str) -> bool:
    """Atomic exists+delete (GETDEL) rather than EXISTS then DEL: two
    concurrent admin requests racing to consume the exact same nonce must
    never both succeed, which a check-then-delete would allow under the
    right interleaving. Returns whether the nonce was found (and is now
    gone either way, once this returns).
    """
    if not nonce:
        return False
    deleted = await redis.getdel(gateway_nonce_key(nonce))
    return deleted is not None


async def require_owner_signature(
    redis: Redis, body: dict[str, Any], action: str, *, client_host: str
) -> dict[str, Any]:
    """The gate for every admin-tier action (br-OWNER-003/005): raises
    `HTTPException(403)` unless `body` carries a `{nonce, signature}` pair
    that verifies against the current owner's public key. Returns the
    ownership record on success, for the caller's own use (rotate needs the
    prior owner's `installedAt`). A plain function called at the top of
    each admin route handler, not a FastAPI dependency -- ported from a
    prior gateway implementation's own `_require_owner_signature`
    verbatim, including why: each admin route needs a different `action`
    label for its own audit-log
    line, and the body itself (not just headers) carries the nonce/
    signature, so there's nothing a `Depends()` would factor out cleanly
    that this function doesn't already do.

    `consume_nonce` runs *before* signature verification, deliberately: a
    wrong-signature attempt still burns that nonce, so retrying the same
    nonce with a different signature can never turn into a brute-force loop
    against one still-valid challenge (matches br-OWNER-003's "single-use",
    not just "single-use on success"). Every rejection path is logged
    individually with its specific reason.
    """
    ownership = await read_ownership(redis)
    if ownership is None:
        logger.warning("gateway.admin_rejected", action=action, reason="no_owner_claimed_yet")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "no owner has claimed this gateway yet")
    nonce = body.get("nonce")
    signature = body.get("signature")
    if not nonce or not signature:
        logger.warning("gateway.admin_rejected", action=action, reason="missing_nonce_or_signature")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "'nonce' and 'signature' are required")
    if not await consume_nonce(redis, nonce):
        logger.warning(
            "gateway.admin_rejected", action=action, reason="invalid_expired_or_reused_nonce"
        )
        raise HTTPException(status.HTTP_403_FORBIDDEN, "invalid, expired, or already-used nonce")
    if not await verify_owner_signature(nonce, signature, ownership["ownerPublicKey"]):
        logger.warning(
            "gateway.admin_rejected",
            action=action,
            reason="signature_did_not_verify",
            owner_label=ownership.get("ownerLabel"),
        )
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "signature did not verify against the owner's public key"
        )
    logger.info(
        "gateway.admin_authorized", action=action, owner_label=ownership.get("ownerLabel"),
        client_host=client_host,
    )
    return ownership


def ownership_fingerprint(owner_public_key: str) -> str:
    return hashlib.sha256(owner_public_key.encode()).hexdigest()


# ── gateway peer-discovery list (br-MESH-001..006, REQ-0070) ───────────────


def split_host_port(addr: str) -> tuple[str, int]:
    host, _, port_str = (addr or "").rpartition(":")
    if host and port_str.isdigit():
        return host, int(port_str)
    return addr or "", 0


def canonical_gateway_key(host: str, port: int) -> str:
    """br-MESH-001: the one identity model every gateway-list entry is keyed
    by -- `lower(host):port`.
    """
    return f"{(host or '').lower()}:{port}"


def self_gateway_entry(self_addr: str, *, now_iso: str) -> dict[str, Any]:
    host, port = split_host_port(self_addr)
    return {
        "host": host,
        "port": port,
        "lastContactAt": now_iso,
        "lastContactResult": "ok",
        "existence": "existing",
    }


def merge_gateway_entry(current: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
    """One incoming (relayed, untrusted) entry merged against this gateway's
    own persisted record for the same canonical key. br-MESH-003/004: a
    brand-new key is always added with `existence` forced to `unknown`,
    regardless of what was reported; an existing key keeps whichever side
    has the more recent `lastContactAt` (ties prefer a verified `existence`
    over `unknown`); and a locally verified `existing`/`absent` is never
    downgraded to a relayed `unknown`, no matter how recent that relayed
    value claims to be -- checked first, ahead of (and overriding) the
    recency comparison.
    """
    if current is None:
        merged = dict(incoming)
        merged["existence"] = "unknown"
        return merged
    current_verified = current.get("existence") in ("existing", "absent")
    incoming_verified = incoming.get("existence") in ("existing", "absent")
    if current_verified and not incoming_verified:
        return current
    incoming_ts = str(incoming.get("lastContactAt") or "")
    current_ts = str(current.get("lastContactAt") or "")
    if incoming_ts > current_ts:
        return dict(incoming)
    if incoming_ts < current_ts:
        return current
    return dict(incoming) if (incoming_verified and not current_verified) else current


async def load_gateway_list(redis: Redis) -> dict[str, Any]:
    """A single JSON blob keyed by each entry's own canonical
    `lower(host):port` string, not a hash: the list is small and bounded
    (br-MESH-005), so one GET/SET beats per-field bookkeeping this data has
    no use for -- unlike telemetry, a gateway list entry never expires on
    its own, it's superseded by a fresher sync instead.
    """
    raw = await redis.get(gateway_list_key())
    return json.loads(raw) if raw is not None else {}


async def save_gateway_list(redis: Redis, entries: dict[str, Any]) -> None:
    await redis.set(gateway_list_key(), json.dumps(entries))


# ── developer Redis CLI (POST /admin/redis-cli) ─────────────────────────────
#
# Unlike everything above, this is a raw, schema-agnostic passthrough --
# intentionally unrestricted (no command allowlist, unlike
# `redis_inspect.py`'s existing `/admin/redis/command`), since it's a
# dev-only tool per its own feature spec, gated by the gateway token rather
# than a data-scoped API key.

#: Simple-status replies are a small, explicit allow-list -- redis-py's
#: `execute_command()` decodes RESP simple-strings and RESP bulk-strings to
#: the exact same Python `str`, so there's no way to tell a status reply
#: ("OK") apart from a same-valued bulk string reply from the value alone.
_REDIS_STATUS_REPLIES = {"OK", "PONG", "QUEUED"}


def build_raw_redis_view(redis: Redis) -> Redis:
    """A second client view over the *same* connection pool (no new
    connections) -- `execute_command()` on a normal client still runs the
    response through redis-py's own per-command response-callback table
    (e.g. SET's callback turns "OK" into a bool), so it's not actually "raw"
    the way a real `redis-cli`'s wire read is. Clearing `response_callbacks`
    on this instance is what makes it genuinely raw. Ported from a prior
    gateway implementation's own `RedisLog`'s `_raw_client` construction
    verbatim.
    """
    raw = Redis(connection_pool=redis.connection_pool)
    raw.response_callbacks = {}
    return raw


async def execute_raw(raw_redis: Redis, *args: str) -> Any:
    return await raw_redis.execute_command(*args)


def redis_type_reply(raw: Any) -> dict[str, Any]:
    """Maps a redis-py `execute_command()` return value to the
    `{type, value}` shape the renderer formats in real-`redis-cli` style.
    """
    if raw is None:
        return {"type": "nil", "value": None}
    if isinstance(raw, bool):
        return {"type": "integer", "value": int(raw)}
    if isinstance(raw, int):
        return {"type": "integer", "value": raw}
    if isinstance(raw, (list, tuple)):
        return {"type": "array", "value": [redis_type_reply(item) for item in raw]}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        if raw in _REDIS_STATUS_REPLIES:
            return {"type": "status", "value": raw}
        return {"type": "bulk", "value": raw}
    return {"type": "bulk", "value": str(raw)}


# ── own-container log gathering (GET /mlog) ─────────────────────────────────


async def _find_own_container(own_container_image_name: str) -> tuple[str, str] | None:
    """(container id, name) for the container running the image named
    `own_container_image_name`, or `None` if this process isn't running
    containerized at all (the embedded/bare-process fallback), or docker
    itself isn't reachable.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "ps",
            "--format",
            "{{json .}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
    except (TimeoutError, OSError):
        return None
    for line in out.decode(errors="replace").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        image = row.get("Image", "")
        if image.split(":")[0].rsplit("/", 1)[-1] == own_container_image_name:
            return row["ID"], row["Names"]
    return None


async def gather_own_container_logs(
    own_container_image_name: str | None, timeout_s: float = 15.0
) -> tuple[str, bytes]:
    """(name, log bytes) for `docker logs` on the gateway's own container --
    "Ship Logs" bundles this alongside the client's own log files. Falls
    back to an explanatory message (not an error) when there's no own
    container to find, or when `own_container_image_name` isn't configured
    at all (see `GatewayConfig.own_container_image_name`'s own docstring
    for why this can't be inferred automatically).
    """
    if own_container_image_name is None:
        return (
            "gateway",
            b"this gateway has no configured own-container image name "
            b"(GatewayConfig.own_container_image_name) -- cannot look up "
            b"its own logs\n",
        )
    found = await _find_own_container(own_container_image_name)
    if found is None:
        return (
            "gateway",
            b"could not find this gateway's own container "
            b"(docker ps found nothing running the "
            + own_container_image_name.encode()
            + b" image -- this server may not be running containerized)\n",
        )
    container_id, name = found
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "logs",
            container_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except (TimeoutError, OSError) as e:
        out = f"could not gather gateway logs: {e}".encode()
    return name, out
