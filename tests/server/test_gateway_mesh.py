"""Migration plan Phase 7: ownership/nonce/gateway-list persistence, the
peer-list merge/canonical-key pure functions, the developer Redis CLI's raw
passthrough, and own-container log gathering.
"""

from __future__ import annotations

from typing import Any

import pytest
from fakeredis import FakeAsyncRedis
from fastapi import HTTPException

from log_sump.server import gateway_mesh

# ── ownership ────────────────────────────────────────────────────────────


async def test_write_ownership_is_first_claim_wins() -> None:
    redis = FakeAsyncRedis()
    first = {"ownerLabel": "alice", "ownerPublicKey": "key-a"}
    second = {"ownerLabel": "bob", "ownerPublicKey": "key-b"}

    assert await gateway_mesh.write_ownership(redis, first) is True
    assert await gateway_mesh.write_ownership(redis, second) is False  # already claimed
    stored = await gateway_mesh.read_ownership(redis)
    assert stored is not None
    assert stored["ownerLabel"] == "alice"


async def test_read_ownership_returns_none_when_unclaimed() -> None:
    redis = FakeAsyncRedis()
    assert await gateway_mesh.read_ownership(redis) is None


async def test_overwrite_ownership_replaces_an_existing_record() -> None:
    redis = FakeAsyncRedis()
    await gateway_mesh.write_ownership(redis, {"ownerLabel": "alice"})

    await gateway_mesh.overwrite_ownership(redis, {"ownerLabel": "bob"})

    stored = await gateway_mesh.read_ownership(redis)
    assert stored is not None
    assert stored["ownerLabel"] == "bob"


def test_ownership_fingerprint_is_a_sha256_hexdigest() -> None:
    fp = gateway_mesh.ownership_fingerprint("some-public-key")
    assert len(fp) == 64
    assert all(c in "0123456789abcdef" for c in fp)


# ── nonces ───────────────────────────────────────────────────────────────


async def test_consume_nonce_is_single_use() -> None:
    redis = FakeAsyncRedis()
    await gateway_mesh.remember_nonce(redis, "nonce-1", 120)

    assert await gateway_mesh.consume_nonce(redis, "nonce-1") is True
    assert await gateway_mesh.consume_nonce(redis, "nonce-1") is False  # already consumed


async def test_consume_unknown_nonce_returns_false() -> None:
    redis = FakeAsyncRedis()
    assert await gateway_mesh.consume_nonce(redis, "never-issued") is False


# ── require_owner_signature ─────────────────────────────────────────────


async def test_require_owner_signature_rejects_when_no_owner_claimed() -> None:
    redis = FakeAsyncRedis()

    with pytest.raises(HTTPException) as exc_info:
        await gateway_mesh.require_owner_signature(
            redis, {"nonce": "n", "signature": "s"}, "ownership.rotate", client_host="1.2.3.4"
        )
    assert exc_info.value.status_code == 403


async def test_require_owner_signature_rejects_missing_nonce_or_signature() -> None:
    redis = FakeAsyncRedis()
    await gateway_mesh.write_ownership(redis, {"ownerLabel": "alice", "ownerPublicKey": "key-a"})

    with pytest.raises(HTTPException) as exc_info:
        await gateway_mesh.require_owner_signature(
            redis, {}, "ownership.rotate", client_host="1.2.3.4"
        )
    assert exc_info.value.status_code == 403


async def test_require_owner_signature_rejects_invalid_nonce() -> None:
    redis = FakeAsyncRedis()
    await gateway_mesh.write_ownership(redis, {"ownerLabel": "alice", "ownerPublicKey": "key-a"})

    with pytest.raises(HTTPException) as exc_info:
        await gateway_mesh.require_owner_signature(
            redis,
            {"nonce": "never-issued", "signature": "s"},
            "ownership.rotate",
            client_host="1.2.3.4",
        )
    assert exc_info.value.status_code == 403


async def test_require_owner_signature_burns_the_nonce_even_on_bad_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """br-OWNER-003: a wrong-signature attempt still consumes the nonce, so
    it can't be retried -- consume_nonce runs before verification.
    """
    redis = FakeAsyncRedis()
    await gateway_mesh.write_ownership(redis, {"ownerLabel": "alice", "ownerPublicKey": "key-a"})
    await gateway_mesh.remember_nonce(redis, "nonce-1", 120)
    monkeypatch.setattr(gateway_mesh, "verify_owner_signature", _fake_verify(False))

    with pytest.raises(HTTPException) as exc_info:
        await gateway_mesh.require_owner_signature(
            redis,
            {"nonce": "nonce-1", "signature": "bad-sig"},
            "ownership.rotate",
            client_host="1.2.3.4",
        )
    assert exc_info.value.status_code == 403
    assert await gateway_mesh.consume_nonce(redis, "nonce-1") is False  # already gone


async def test_require_owner_signature_succeeds_and_returns_ownership_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeAsyncRedis()
    record = {"ownerLabel": "alice", "ownerPublicKey": "key-a"}
    await gateway_mesh.write_ownership(redis, record)
    await gateway_mesh.remember_nonce(redis, "nonce-1", 120)
    monkeypatch.setattr(gateway_mesh, "verify_owner_signature", _fake_verify(True))

    result = await gateway_mesh.require_owner_signature(
        redis, {"nonce": "nonce-1", "signature": "good-sig"}, "ownership.rotate", client_host="?"
    )

    assert result["ownerLabel"] == "alice"


def _fake_verify(result: bool):
    async def _verify(nonce: str, signature: str, owner_public_key: str) -> bool:
        return result

    return _verify


# ── peer-discovery list: pure functions ─────────────────────────────────


def test_split_host_port() -> None:
    assert gateway_mesh.split_host_port("10.0.0.5:8080") == ("10.0.0.5", 8080)
    assert gateway_mesh.split_host_port("no-port-here") == ("no-port-here", 0)
    assert gateway_mesh.split_host_port("") == ("", 0)


def test_canonical_gateway_key_lowercases_host() -> None:
    assert gateway_mesh.canonical_gateway_key("Some-Host.example", 8080) == "some-host.example:8080"


def test_self_gateway_entry() -> None:
    entry = gateway_mesh.self_gateway_entry("10.0.0.1:8080", now_iso="2026-08-15T00:00:00.000Z")
    assert entry == {
        "host": "10.0.0.1",
        "port": 8080,
        "lastContactAt": "2026-08-15T00:00:00.000Z",
        "lastContactResult": "ok",
        "existence": "existing",
    }


def test_merge_gateway_entry_new_key_forces_unknown_existence() -> None:
    incoming = {"host": "h", "port": 1, "lastContactAt": "z", "existence": "existing"}
    merged = gateway_mesh.merge_gateway_entry(None, incoming)
    assert merged["existence"] == "unknown"


def test_merge_gateway_entry_verified_never_downgraded_by_relayed_unknown() -> None:
    current = {"existence": "existing", "lastContactAt": "2026-08-14T00:00:00Z"}
    incoming = {"existence": "unknown", "lastContactAt": "2026-08-15T00:00:00Z"}  # more recent!

    merged = gateway_mesh.merge_gateway_entry(current, incoming)

    assert merged is current  # recency never overrides "verified beats relayed-unknown"


def test_merge_gateway_entry_more_recent_wins_when_both_verified() -> None:
    current = {"existence": "existing", "lastContactAt": "2026-08-14T00:00:00Z"}
    incoming = {"existence": "absent", "lastContactAt": "2026-08-15T00:00:00Z"}

    merged = gateway_mesh.merge_gateway_entry(current, incoming)

    assert merged is not current
    assert merged["existence"] == "absent"


def test_merge_gateway_entry_tie_prefers_verified() -> None:
    current = {"existence": "unknown", "lastContactAt": "2026-08-14T00:00:00Z"}
    incoming = {"existence": "existing", "lastContactAt": "2026-08-14T00:00:00Z"}

    merged = gateway_mesh.merge_gateway_entry(current, incoming)

    assert merged["existence"] == "existing"


# ── peer-discovery list: redis-backed ────────────────────────────────────


async def test_load_gateway_list_defaults_to_empty() -> None:
    redis = FakeAsyncRedis()
    assert await gateway_mesh.load_gateway_list(redis) == {}


async def test_save_and_load_gateway_list_roundtrips() -> None:
    redis = FakeAsyncRedis()
    entries = {"10.0.0.1:8080": {"host": "10.0.0.1", "port": 8080, "existence": "existing"}}

    await gateway_mesh.save_gateway_list(redis, entries)

    assert await gateway_mesh.load_gateway_list(redis) == entries


# ── developer Redis CLI passthrough ──────────────────────────────────────


async def test_execute_raw_and_redis_type_reply_status() -> None:
    redis = FakeAsyncRedis()
    raw_view = gateway_mesh.build_raw_redis_view(redis)

    raw = await gateway_mesh.execute_raw(raw_view, "SET", "some-key", "some-value")

    assert gateway_mesh.redis_type_reply(raw) == {"type": "status", "value": "OK"}


async def test_execute_raw_bulk_string_reply() -> None:
    redis = FakeAsyncRedis()
    raw_view = gateway_mesh.build_raw_redis_view(redis)
    await redis.set("some-key", "some-value")

    raw = await gateway_mesh.execute_raw(raw_view, "GET", "some-key")

    assert gateway_mesh.redis_type_reply(raw) == {"type": "bulk", "value": "some-value"}


async def test_execute_raw_nil_reply() -> None:
    redis = FakeAsyncRedis()
    raw_view = gateway_mesh.build_raw_redis_view(redis)

    raw = await gateway_mesh.execute_raw(raw_view, "GET", "does-not-exist")

    assert gateway_mesh.redis_type_reply(raw) == {"type": "nil", "value": None}


def test_redis_type_reply_array() -> None:
    assert gateway_mesh.redis_type_reply([1, "OK", None]) == {
        "type": "array",
        "value": [
            {"type": "integer", "value": 1},
            {"type": "status", "value": "OK"},
            {"type": "nil", "value": None},
        ],
    }


# ── own-container log gathering ──────────────────────────────────────────


class _FakeProc:
    def __init__(self, stdout: bytes = b"", returncode: int = 0) -> None:
        self._stdout = stdout
        self.returncode = returncode

    async def communicate(self, input: bytes | None = None) -> tuple[bytes, bytes]:  # noqa: A002
        return self._stdout, b""


def _patch_docker_subprocess(
    monkeypatch: pytest.MonkeyPatch, *, ps_output: bytes, logs_output: bytes
) -> None:
    async def _fake_exec(*args: str, **kwargs: Any) -> _FakeProc:
        if args[:2] == ("docker", "ps"):
            return _FakeProc(stdout=ps_output)
        if args[:2] == ("docker", "logs"):
            return _FakeProc(stdout=logs_output)
        raise AssertionError(f"unexpected subprocess call: {args}")

    monkeypatch.setattr(gateway_mesh.asyncio, "create_subprocess_exec", _fake_exec)


async def test_gather_own_container_logs_returns_explanatory_message_when_unconfigured() -> None:
    name, data = await gateway_mesh.gather_own_container_logs(None)

    assert name == "gateway"
    assert b"no configured own-container image name" in data


async def test_gather_own_container_logs_falls_back_when_no_container_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_docker_subprocess(monkeypatch, ps_output=b"", logs_output=b"")

    name, data = await gateway_mesh.gather_own_container_logs("test-gateway")

    assert name == "gateway"
    assert b"could not find this gateway's own container" in data


async def test_gather_own_container_logs_finds_and_fetches_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ps_line = b'{"ID":"abc123","Names":"my-gateway","Image":"test-gateway:latest"}\n'
    _patch_docker_subprocess(
        monkeypatch, ps_output=ps_line, logs_output=b"hello from the gateway\n"
    )

    name, data = await gateway_mesh.gather_own_container_logs("test-gateway")

    assert name == "my-gateway"
    assert data == b"hello from the gateway\n"


async def test_gather_own_container_logs_ignores_containers_with_a_different_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ps_line = b'{"ID":"abc123","Names":"unrelated","Image":"nginx:latest"}\n'
    _patch_docker_subprocess(monkeypatch, ps_output=ps_line, logs_output=b"")

    name, data = await gateway_mesh.gather_own_container_logs("test-gateway")

    assert name == "gateway"
    assert b"could not find this gateway's own container" in data
