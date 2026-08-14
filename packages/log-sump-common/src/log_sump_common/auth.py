"""Client authentication/authorization for log-server (spec §9).

Clients authenticate with an API key; authorization is per daemon. Kept
behind an `AuthBackend` Protocol so the storage/verification mechanism can be
swapped later without touching request handlers.
"""

from __future__ import annotations

from typing import Protocol

from redis.asyncio import Redis

from log_sump_common.redis_keys import auth_key


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
