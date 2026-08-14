"""Single source of truth for every Redis key format log-sump uses.

Keeping key-naming in one place means the listener/consumer/trimmer/query
paths (and their tests) can never drift apart on how a key is built.
"""

from __future__ import annotations

import hashlib

from log_sump_common.schema import Kind

_PREFIX = "logsump"

#: Versioned so a future schema change can run a new version alongside the
#: old one instead of requiring a synchronized cutover.
INGEST_LIST = f"{_PREFIX}:ingest:v1"


def stream_key(docker_host: str, kind: Kind) -> str:
    """Per-daemon, per-kind Redis Stream key.

    Split by `kind` (not by container) so `XTRIM MINID` can enforce
    `retention_days` and `metrics_retention_days` independently and exactly —
    see spec §7. Not split by container: container IDs churn on every
    redeploy, which would accumulate stale, permanently-empty stream keys.
    """
    return f"{_PREFIX}:stream:{docker_host}:{kind.value}"


def daemon_status_key(docker_host: str) -> str:
    """Hash of `{reachable, last_listing_ts, container_count}` for one daemon."""
    return f"{_PREFIX}:daemon:{docker_host}:status"


def daemons_key() -> str:
    """Hash of daemon_id -> JSON-encoded `DaemonConfig`, for daemons
    registered at runtime (migration plan Phase 3) -- YAML's `daemons:`
    list seeds the boot-time set; this hash holds what's been added/
    removed via the admin API since. See `log_sump_common.daemon_registry`.
    """
    return f"{_PREFIX}:daemons"


def auth_key(api_key: str) -> str:
    """Set of permitted `docker_host` values for a given API key.

    Keyed by a hash of the credential, never the raw key, so a Redis dump/backup
    doesn't expose live credentials.
    """
    digest = hashlib.sha256(api_key.encode()).hexdigest()
    return f"{_PREFIX}:auth:{digest}"
