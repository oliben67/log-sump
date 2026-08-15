"""Runtime-mutable daemon registry (migration plan Phase 3): YAML's
`Settings.daemons` list seeds the boot-time set; this Redis hash holds
additions/removals made after boot, via `log_sump.server`'s `/daemons`
admin endpoints -- the `/docker/collect`/`/docker/forget`/"Set Sources" gap.

Deliberately the one place `log-listener` reads Redis directly, rather than
staying data-flow-only like every other listener task (contrast
`services_listing.py`'s own docstring, which explains why *that* one
avoids it): there is no other channel for a runtime daemon addition to
reach `log-listener` at all -- the Logstash pipeline only ever carries
data *out* of `log-listener`, never config *in* -- and Redis is the one
piece of state every process already shares (`config.py`'s `RedisConfig`).
Read-mostly from `log-listener`'s side: it only ever lists this hash;
only `log_sump.server`'s admin endpoints write to it.
"""

from __future__ import annotations

import json

from redis.asyncio import Redis

from .config import DaemonConfig
from .redis_keys import daemons_key


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


async def list_registered_daemons(redis: Redis) -> list[DaemonConfig]:
    raw = await redis.hgetall(daemons_key())
    return [DaemonConfig(**json.loads(_decode(v))) for v in raw.values()]


async def register_daemon(redis: Redis, daemon: DaemonConfig) -> None:
    await redis.hset(daemons_key(), daemon.id, daemon.model_dump_json())


async def unregister_daemon(redis: Redis, daemon_id: str) -> bool:
    """True if `daemon_id` was actually registered (and is now removed)."""
    removed = await redis.hdel(daemons_key(), daemon_id)
    return bool(removed)
