"""Read-only Redis inspection: lets an authenticated client run a curated
set of read-only commands directly against the Redis store — e.g. to check
what's actually in a stream or key without going through the higher-level
`/records` API. This is developer-facing debugging access, so its safety
comes from *what* it can run, not *who* can run it:

- **Command surface**: a fixed allowlist of inspection-only commands.
  Writes, `FLUSHALL`/`FLUSHDB`, `CONFIG`, `SHUTDOWN`, replication/cluster
  commands, and everything else not explicitly listed are rejected
  outright, regardless of who's asking — confirmed scope, not a default
  assumed unprompted.
- **Who can call it**: any client with a valid API key (see
  `deps.require_valid_api_key`) — also confirmed scope. The command
  allowlist is what keeps this safe; gating it behind a separate elevated
  credential tier would add operational overhead (provisioning yet another
  kind of key) without changing what a compromised key could actually do
  here, since even a fully "admin" key still can't run anything off this
  list.
"""

from __future__ import annotations

ALLOWED_COMMANDS: frozenset[str] = frozenset(
    {
        "GET",
        "MGET",
        "EXISTS",
        "TYPE",
        "TTL",
        "PTTL",
        "STRLEN",
        "KEYS",
        "SCAN",
        "DBSIZE",
        "HGET",
        "HMGET",
        "HGETALL",
        "HKEYS",
        "HVALS",
        "HLEN",
        "HEXISTS",
        "SMEMBERS",
        "SCARD",
        "SISMEMBER",
        "LRANGE",
        "LLEN",
        "LINDEX",
        "ZRANGE",
        "ZSCORE",
        "ZCARD",
        "ZRANK",
        "XRANGE",
        "XREVRANGE",
        "XLEN",
        "XINFO",
        "PING",
        "INFO",
        "MEMORY",
    }
)


class CommandNotAllowed(ValueError):
    def __init__(self, command: str) -> None:
        super().__init__(f"command {command!r} is not in the read-only allowlist")
        self.command = command


def check_command_allowed(command: str) -> str:
    """Returns the normalized (upper-cased) command name, or raises `CommandNotAllowed`."""
    normalized = command.strip().upper()
    if normalized not in ALLOWED_COMMANDS:
        raise CommandNotAllowed(command)
    return normalized
