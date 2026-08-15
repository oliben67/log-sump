from log_sump.common.redis_keys import INGEST_LIST, auth_key, daemon_status_key, stream_key
from log_sump.common.schema import Kind


def test_stream_key_splits_by_daemon_and_kind() -> None:
    assert stream_key("daemon-a", Kind.LOG) == "logsump:stream:daemon-a:log"
    assert stream_key("daemon-a", Kind.METRIC) == "logsump:stream:daemon-a:metric"
    assert stream_key("daemon-a", Kind.LOG) != stream_key("daemon-b", Kind.LOG)


def test_ingest_list_is_versioned() -> None:
    assert INGEST_LIST == "logsump:ingest:v1"


def test_daemon_status_key() -> None:
    assert daemon_status_key("daemon-a") == "logsump:daemon:daemon-a:status"


def test_auth_key_does_not_embed_raw_api_key() -> None:
    key = auth_key("super-secret-token")
    assert "super-secret-token" not in key
    assert key.startswith("logsump:auth:")


def test_auth_key_is_deterministic() -> None:
    assert auth_key("token-a") == auth_key("token-a")
    assert auth_key("token-a") != auth_key("token-b")
