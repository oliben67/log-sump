import subprocess
from pathlib import Path

from fakeredis import FakeAsyncRedis
from log_sump_common.auth import (
    GatewayTokenAuthBackend,
    RedisApiKeyAuthBackend,
    verify_owner_signature,
)
from log_sump_common.redis_keys import auth_key


async def test_unknown_api_key_returns_none() -> None:
    redis = FakeAsyncRedis()
    backend = RedisApiKeyAuthBackend(redis)
    assert await backend.permitted_daemons("does-not-exist") is None


async def test_empty_api_key_returns_none() -> None:
    backend = RedisApiKeyAuthBackend(FakeAsyncRedis())
    assert await backend.permitted_daemons("") is None


async def test_known_api_key_returns_permitted_daemons() -> None:
    redis = FakeAsyncRedis()
    await redis.sadd(auth_key("valid-token"), "daemon-a", "daemon-b")
    backend = RedisApiKeyAuthBackend(redis)

    permitted = await backend.permitted_daemons("valid-token")

    assert permitted == frozenset({"daemon-a", "daemon-b"})


def test_gateway_token_backend_unset_accepts_anything() -> None:
    backend = GatewayTokenAuthBackend(None)

    assert backend.configured is False
    assert backend.is_valid(None) is True
    assert backend.is_valid("literally-anything") is True


def test_gateway_token_backend_configured_requires_exact_match() -> None:
    backend = GatewayTokenAuthBackend("shared-secret")

    assert backend.configured is True
    assert backend.is_valid("shared-secret") is True
    assert backend.is_valid("wrong") is False
    assert backend.is_valid(None) is False
    assert backend.is_valid("") is False


def _ssh_keygen_sign(
    tmp_path: Path, *, key_name: str, namespace: str, data: bytes
) -> tuple[str, str]:
    """Generates a fresh ed25519 keypair and signs `data` with it via the
    real `ssh-keygen -Y sign` -- returns (public_key, sshsig_armor).
    """
    key_path = tmp_path / key_name
    subprocess.run(
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(key_path)],
        check=True,
        capture_output=True,
    )
    public_key = key_path.with_suffix(".pub").read_text()
    data_file = tmp_path / f"{key_name}.data"
    data_file.write_bytes(data)
    subprocess.run(
        ["ssh-keygen", "-Y", "sign", "-f", str(key_path), "-n", namespace, str(data_file)],
        check=True,
        capture_output=True,
    )
    signature = data_file.with_suffix(data_file.suffix + ".sig").read_text()
    return public_key, signature


async def test_verify_owner_signature_accepts_a_genuine_signature(tmp_path: Path) -> None:
    """Round-trips through the real `ssh-keygen -Y sign`/`verify` pair --
    the SSHSIG format is the whole reason this doesn't use paramiko (see
    `verify_owner_signature`'s own docstring), so this is worth confirming
    against the actual tool, not a mock of it.
    """
    nonce = "test-nonce-123"
    public_key, signature = _ssh_keygen_sign(
        tmp_path, key_name="owner", namespace="cttc-admin-auth", data=nonce.encode()
    )

    assert await verify_owner_signature(nonce, signature, public_key) is True


async def test_verify_owner_signature_rejects_signature_from_a_different_key(
    tmp_path: Path,
) -> None:
    nonce = "test-nonce-123"
    _other_public_key, signature = _ssh_keygen_sign(
        tmp_path, key_name="impostor", namespace="cttc-admin-auth", data=nonce.encode()
    )
    real_owner_public_key, _ = _ssh_keygen_sign(
        tmp_path, key_name="real-owner", namespace="cttc-admin-auth", data=b"unrelated"
    )

    assert await verify_owner_signature(nonce, signature, real_owner_public_key) is False


async def test_verify_owner_signature_rejects_a_tampered_nonce(tmp_path: Path) -> None:
    public_key, signature = _ssh_keygen_sign(
        tmp_path, key_name="owner", namespace="cttc-admin-auth", data=b"original-nonce"
    )

    assert await verify_owner_signature("a-different-nonce", signature, public_key) is False


async def test_verify_owner_signature_rejects_garbage_signature() -> None:
    result = await verify_owner_signature("nonce", "not a real signature", "ssh-ed25519 AAAA fake")
    assert result is False
