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
    ssh_options: list[str] = Field(default_factory=list)
    enabled: bool = True


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
    """User transform plugins (migration plan Phase 5) -- log-only,
    matching cttc's own `TransformRegistry`. `directory` unset (the
    default) disables transforms entirely: no directory to scan, nothing
    applied.
    """

    directory: str | None = None
    #: Names loaded and applied, in order, to every live-collected
    #: LogRecord (see ingest/consumer.py). Unlike cttc (which selects
    #: transforms per opened source), this is a single global list --
    #: per-daemon selection is a documented simplification for this pass.
    active: list[str] = Field(default_factory=list)


class Settings(BaseSettings):
    daemons: list[DaemonConfig] = Field(default_factory=list)
    listener: ListenerConfig = Field(default_factory=ListenerConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    logstash: LogstashConfig = Field(default_factory=LogstashConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    transforms: TransformsConfig = Field(default_factory=TransformsConfig)

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
