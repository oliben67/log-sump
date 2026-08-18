from pathlib import Path

import pytest

from log_sump.common.config import CONFIG_FILE_ENV_VAR, Settings


def test_defaults_with_no_yaml_or_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONFIG_FILE_ENV_VAR, raising=False)
    settings = Settings()

    assert settings.daemons == []
    assert settings.retention.retention_days == 7
    assert settings.redis.url == "redis://127.0.0.1:16379/0"


def test_loads_daemons_from_yaml_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
daemons:
  - id: prod-a
    host: 10.0.0.5
    user: deploy
    transport: ssh
  - id: dev
    host: localhost
    transport: local
    enabled: false
retention:
  retention_days: 14
"""
    )
    monkeypatch.setenv(CONFIG_FILE_ENV_VAR, str(config_file))

    settings = Settings()

    assert [d.id for d in settings.daemons] == ["prod-a", "dev"]
    assert settings.enabled_daemons() == [d for d in settings.daemons if d.id == "prod-a"]
    assert settings.retention.retention_days == 14


def test_env_var_overrides_yaml_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text("retention:\n  retention_days: 14\n")
    monkeypatch.setenv(CONFIG_FILE_ENV_VAR, str(config_file))
    monkeypatch.setenv("LOG_SUMP_RETENTION__RETENTION_DAYS", "30")

    settings = Settings()

    assert settings.retention.retention_days == 30


def test_redis_port_is_overridable_via_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CONFIG_FILE_ENV_VAR, raising=False)
    monkeypatch.setenv("LOG_SUMP_REDIS__PORT", "23456")

    settings = Settings()

    assert settings.redis.port == 23456
    assert settings.redis.url == "redis://127.0.0.1:23456/0"


def test_metrics_retention_days_falls_back_to_retention_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CONFIG_FILE_ENV_VAR, raising=False)
    settings = Settings()
    settings.retention.retention_days = 7
    assert settings.retention.effective_metrics_retention_days() == 7

    settings.retention.metrics_retention_days = 3
    assert settings.retention.effective_metrics_retention_days() == 3


def test_system_stats_interval_falls_back_to_stats_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CONFIG_FILE_ENV_VAR, raising=False)
    settings = Settings()
    settings.listener.stats_interval_s = 15.0
    assert settings.listener.effective_system_stats_interval_s() == 15.0

    settings.listener.system_stats_interval_s = 60.0
    assert settings.listener.effective_system_stats_interval_s() == 60.0
