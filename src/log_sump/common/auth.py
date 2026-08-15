"""Client authentication/authorization for log-server (spec §9).

Clients authenticate with an API key; authorization is per daemon. Kept
behind an `AuthBackend` Protocol so the storage/verification mechanism can be
swapped later without touching request handlers.

Migration plan Phase 7 adds two more, unrelated tiers alongside the
per-daemon one above -- both additive, neither replacing
`RedisApiKeyAuthBackend`:

- `GatewayTokenAuthBackend`: cttc's own unscoped shared-secret gateway
  token (`X-CTTC-Token`), for the new gateway-mesh/admin routes
  (`log_sump.server.routers.gateway`) that have no per-daemon concept at
  all -- a whole-gateway identity/ownership action isn't scoped to any one
  `docker_host`.
- `verify_owner_signature`: proves a request was authorized by the
  gateway's *current owner*, for the admin-tier actions gated by
  `log_sump.server.gateway_mesh.require_owner_signature` (ownership
  rotation). Ported from cttc's own `_verify_owner_signature` verbatim.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Protocol

from redis.asyncio import Redis

from log_sump.common.redis_keys import auth_key

#: The `-n` namespace `ssh-keygen -Y sign`/`verify` both must agree on --
#: scopes a signature to this specific purpose, so a signature produced for
#: some other `ssh-keygen -Y` consumer (e.g. git commit signing with the
#: same key) could never be replayed here, and vice versa. Matches cttc's
#: own `ADMIN_SIGNATURE_NAMESPACE` exactly -- an existing owner's already-
#: issued signing setup must keep working unchanged across the migration.
ADMIN_SIGNATURE_NAMESPACE = "cttc-admin-auth"


class AuthBackend(Protocol):
    async def permitted_daemons(self, api_key: str) -> frozenset[str] | None:
        """Daemon ids this key may access, or `None` if the key is unknown/empty."""
        ...


class RedisApiKeyAuthBackend:
    """Looks up `api_key -> {permitted docker_host ids}` in a Redis Set.

    Provisioning (adding/revoking keys under `redis_keys.auth_key`) is an
    operational concern, not part of the request path.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def permitted_daemons(self, api_key: str) -> frozenset[str] | None:
        if not api_key:
            return None
        members: set[bytes | str] = await self._redis.smembers(auth_key(api_key))
        if not members:
            return None
        return frozenset(m.decode() if isinstance(m, bytes) else m for m in members)


class GatewayTokenAuthBackend:
    """Unscoped shared-secret gateway token -- cttc's own auth model (one
    token protects the whole gateway), unlike `RedisApiKeyAuthBackend`'s
    per-daemon scoping above. Configured (from `Settings.gateway.token`),
    not Redis-provisioned: a single network-perimeter secret set once at
    deploy time (matches cttc's own `CTTC_API_TOKEN`) doesn't need a
    revocable-credential store the way per-client API keys do.

    Unset (`token=None`, the default) means "no gate" -- exactly as
    permissive as an embedded, never-network-reachable "This machine"
    gateway already is, matching cttc's own `_require_api_token` docstring:
    "this only ever tightens a deployment that opted into being reachable
    from the network in the first place."
    """

    def __init__(self, token: str | None) -> None:
        self._token = token

    @property
    def configured(self) -> bool:
        return bool(self._token)

    def is_valid(self, presented: str | None) -> bool:
        if not self._token:
            return True
        return presented == self._token


async def verify_owner_signature(nonce: str, signature: str, owner_public_key: str) -> bool:
    """Verifies `signature` -- an `ssh-keygen -Y sign` SSHSIG armor blob --
    over `nonce`, against `owner_public_key`, by shelling out to
    `ssh-keygen -Y verify`. Ported from cttc's own `_verify_owner_signature`
    verbatim, including why: NOT paramiko, despite it already being a
    dependency for Docker-host SSH transport (`transport.py`'s
    `SSHTransport`) -- paramiko's own signature verification speaks the raw
    SSH auth-protocol wire format (RFC 4252/8332), not the SSHSIG envelope
    `ssh-keygen -Y sign` produces (the same format `git commit -S` uses);
    paramiko has no SSHSIG parser, and hand-rolling one would be exactly the
    kind of crypto-adjacent risk this codebase avoids elsewhere in favor of
    shelling to the real OS tool. `openssh-client` is already in the
    container image (`docker/Dockerfile`), for `SSHTransport`.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        allowed_signers = tmp_path / "allowed_signers"
        allowed_signers.write_text(f"owner {owner_public_key}\n")
        sig_file = tmp_path / "nonce.sig"
        sig_file.write_text(signature)
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh-keygen",
                "-Y",
                "verify",
                "-f",
                str(allowed_signers),
                "-I",
                "owner",
                "-n",
                ADMIN_SIGNATURE_NAMESPACE,
                "-s",
                str(sig_file),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            return False
        try:
            await asyncio.wait_for(proc.communicate(nonce.encode()), timeout=5.0)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return False
        return proc.returncode == 0
