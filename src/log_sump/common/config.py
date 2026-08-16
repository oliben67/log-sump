"""Config loading for log-sump (spec §8).

One `Settings` model shared by log-listener and log-server. Values load from
a YAML file (the daemon catalog, tunables) layered under environment
variable overrides (secrets: Redis auth, API keys — never committed to
YAML). Point at a YAML file via the `LOG_SUMP_CONFIG_FILE` env var; env vars
use the `LOG_SUMP_` prefix with `__` as the nested-field delimiter, e.g.
`LOG_SUMP_REDIS__URL=redis://...` or `LOG_SUMP_RETENTION__RETENTION_DAYS=14`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

CONFIG_FILE_ENV_VAR = "LOG_SUMP_CONFIG_FILE"


class DaemonConfig(BaseModel):
    id: str
    host: str
    user: str = "root"
    transport: Literal["ssh", "local"] = "ssh"
    #: `SSHTransport`'s port (paramiko, not the system `ssh` binary -- see
    #: common/transport.py's module docstring). Was a free-form
    #: `ssh_options: list[str]` CLI-flag passthrough until 2026-08-16;
    #: paramiko takes structured connect() kwargs, not raw flags, so this
    #: narrowed to the one thing that passthrough was actually used for.
    port: int = 22
    enabled: bool = True
    #: Selective collection (migration plan Phase 9's "Set Sources" gap):
    #: `None` (default) watches every container on this daemon, matching
    #: every prior phase's behavior unchanged. A list restricts log
    #: tailing (`listener/app.py`'s new-container dispatcher) and
    #: per-container stats sampling (`container_stats.py`) to containers
    #: whose name is in it -- an empty list is a valid, different state
    #: from `None`: "registered, watching nothing yet," e.g. right after a
    #: client adds a daemon before picking any containers. Addressed by
    #: name, not id, matching how a client would typically reference a
    #: container it already knows about. Swarm *services* aren't covered
    #: by this (log-sump has no per-service log tailing at all yet --
    #: services_listing.py is discovery-only).
    watched_containers: list[str] | None = None


class ListenerConfig(BaseModel):
    listing_interval_s: float = 5.0
    tracker_interval_s: float = 10.0
    missing_threshold_cycles: int = 3
    stats_interval_s: float = 15.0
    #: Falls back to `stats_interval_s` when unset — see `effective_system_stats_interval_s`.
    system_stats_interval_s: float | None = None
    max_concurrent_listener_spawns: int = 10
    #: How often the runtime daemon registry (migration plan Phase 3,
    #: `daemon_registry.py`) is polled for additions/removals made via the
    #: admin API. Its own setting, not reused from listing_interval_s --
    #: adding a daemon isn't as latency-sensitive as discovering a new
    #: container on an already-watched one.
    daemon_registry_poll_interval_s: float = 5.0
    #: Where `system_stats.py` reads host `/proc` files from, for
    #: `transport: local` daemons only (an SSH-reached daemon's `/proc`
    #: read runs on *that* remote machine, always at the literal `/proc` --
    #: this setting never applies there). Matters when this process itself
    #: runs containerized (spec §3.2's four-process image) without a
    #: container-local PID namespace of its own to see the true host
    #: through: a process-supervisor-owning container can't use
    #: `pid: host` (its PID-1 requirement conflicts with sharing the
    #: host's PID namespace), so accurate host-wide `/proc` visibility has
    #: to come from a plain bind mount instead (the same pattern
    #: `node_exporter`/`cAdvisor` use) -- override to wherever that mount
    #: lands, e.g. `/host/proc`.
    local_proc_root: str = "/proc"

    def effective_system_stats_interval_s(self) -> float:
        return self.system_stats_interval_s or self.stats_interval_s


class MetricsConfig(BaseModel):
    enabled: bool = True
    system_enabled: bool = True
    cpu_enabled: bool = True
    memory_enabled: bool = True
    disk_enabled: bool = True
    network_enabled: bool = True
    system_metrics_source: Literal["proc", "container-aggregate", "none"] = "proc"


class RetentionConfig(BaseModel):
    retention_days: int = 7
    #: Falls back to `retention_days` when unset — see `effective_metrics_retention_days`.
    metrics_retention_days: int | None = None
    trim_interval_seconds: float = 300.0

    def effective_metrics_retention_days(self) -> int:
        return (
            self.metrics_retention_days
            if self.metrics_retention_days is not None
            else self.retention_days
        )


class LogstashConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5959
    #: `python-logstash-async` buffer DB — persistent by default so in-flight
    #: records survive a listener restart (spec §5.2 / §11.4).
    database_path: str = "/data/logstash-async-buffer.db"


class RedisConfig(BaseModel):
    url: str = "redis://127.0.0.1:6379/0"


class ServerConfig(BaseModel):
    bind_host: str = "0.0.0.0"
    bind_port: int = 8080
    page_size_default: int = 200
    page_size_max: int = 1000
    #: How often sessions/buffers/scheduler are ticked (migration plan
    #: Phase 4) -- cheap, in-memory-only sweeps; the actual Redis I/O only
    #: happens when a session/buffer's window is actually exported, not on
    #: every tick.
    tick_interval_seconds: float = 5.0
    #: Same cadence, for events.py's condition checks (Phase 5) -- kept as
    #: its own setting since it's a genuinely different cost profile (a
    #: real Redis XRANGE per condition per tick, not an in-memory sweep).
    events_tick_interval_seconds: float = 5.0


class TransformsConfig(BaseModel):
    """User transform plugins (migration plan Phase 5) -- log-only.
    `directory` unset (the default) disables transforms entirely: no
    directory to scan, nothing applied.
    """

    directory: str | None = None
    #: Names loaded and applied, in order, to every live-collected
    #: LogRecord (see ingest/consumer.py). A single global list, not
    #: selected per daemon -- per-daemon selection is a documented
    #: simplification for this pass.
    active: list[str] = Field(default_factory=list)


class PluginsConfig(BaseModel):
    """Optional router plugins -- log-sump's generic extension point for
    whatever a particular deployment needs beyond the built-in API surface.
    `directory` unset (the default) disables plugin loading entirely: no
    directory to scan, nothing mounted. See
    `log_sump.server.plugins`' own module docstring for the plugin
    contract itself.
    """

    directory: str | None = None


class GatewayConfig(BaseModel):
    """Gateway-mesh + admin-auth tunables (migration plan Phase 7). Plain
    `Settings` fields, under the standard `LOG_SUMP_GATEWAY__*` env var
    namespace, rather than argparse flags or raw unprefixed env vars --
    log-sump has no argparse-based CLI at all.
    """

    #: Shared secret required (as the `X-CTTC-Token` header, or a `?token=`
    #: query param for the one client -- browser EventSource -- that can't
    #: set a custom header) on every gateway-mesh/admin route when set.
    #: Unset (the default) leaves those routes exactly as unauthenticated
    #: as an embedded, never-network-reachable "This machine" gateway
    #: already is.
    token: str | None = None
    #: Overrides the inferred self-address (`Host` header) used for this
    #: gateway's own entry in the peer-discovery list.
    public_address: str | None = None
    admin_nonce_ttl_seconds: float = 120.0
    gateway_list_max_entries: int = 500
    #: The image name `GET /mlog` matches against `docker ps` output to
    #: find the container this gateway process itself is running in, so it
    #: can bundle its own logs alongside a client's ("Ship Logs"). Unset
    #: (the default) disables that lookup entirely -- there's no way to
    #: reliably identify "this process's own container" without being told
    #: what image it runs under, and log-sump has no deployment-agnostic
    #: way to guess. A deployment that builds and runs its own image under
    #: a fixed name sets this to that name.
    own_container_image_name: str | None = None


class Settings(BaseSettings):
    daemons: list[DaemonConfig] = Field(default_factory=list)
    listener: ListenerConfig = Field(default_factory=ListenerConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    logstash: LogstashConfig = Field(default_factory=LogstashConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    transforms: TransformsConfig = Field(default_factory=TransformsConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    plugins: PluginsConfig = Field(default_factory=PluginsConfig)

    model_config = SettingsConfigDict(
        env_prefix="LOG_SUMP_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    def enabled_daemons(self) -> list[DaemonConfig]:
        return [d for d in self.daemons if d.enabled]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Priority order: earlier sources override later ones. Env vars sit
        # above the YAML file so secrets/overrides never need to touch it.
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings, dotenv_settings]
        yaml_path = os.environ.get(CONFIG_FILE_ENV_VAR)
        if yaml_path and Path(yaml_path).is_file():
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=yaml_path))
        sources.append(file_secret_settings)
        return tuple(sources)


def load_settings() -> Settings:
    """Load settings from `$LOG_SUMP_CONFIG_FILE` (if set) plus env overrides."""
    return Settings()
