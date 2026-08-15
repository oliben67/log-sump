"""HTTP-level tests for the gateway-mesh/admin routes (migration plan
Phase 7): `/ping`, `/gateways/sync`, ownership claim/challenge/rotate,
`/mlog`, `/admin/redis-cli`, `/shutdown`.
"""

from __future__ import annotations

import asyncio
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient

from log_sump.common.config import GatewayConfig, Settings
from log_sump.server.app import create_app

TOKEN = "gateway-secret"


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    yield FakeAsyncRedis()


@pytest.fixture
async def client(redis: FakeAsyncRedis) -> AsyncIterator[AsyncClient]:
    """No gateway token configured -- matches the embedded, never-network-
    reachable default; gateway-mesh routes are unauthenticated here, same
    as cttc's own default.
    """
    app = create_app(settings=Settings(), redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


@pytest.fixture
async def gated_client(redis: FakeAsyncRedis) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=Settings(gateway=GatewayConfig(token=TOKEN)), redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def _token_headers() -> dict[str, str]:
    return {"X-CTTC-Token": TOKEN}


# ── /ping ─────────────────────────────────────────────────────────────────


async def test_ping_answers_unauthenticated_even_when_a_token_is_configured(
    gated_client: AsyncClient,
) -> None:
    resp = await gated_client.get("/ping")
    assert resp.status_code == 200
    assert resp.json()["service"] == "gateway"


# ── gateway token gate ──────────────────────────────────────────────────


async def test_gateways_sync_requires_token_when_configured(gated_client: AsyncClient) -> None:
    resp = await gated_client.post("/gateways/sync", json={"entries": []})
    assert resp.status_code == 401


async def test_gateways_sync_accepts_token_via_header(gated_client: AsyncClient) -> None:
    resp = await gated_client.post(
        "/gateways/sync", json={"entries": []}, headers=_token_headers()
    )
    assert resp.status_code == 200


async def test_gateways_sync_accepts_token_via_query_param(gated_client: AsyncClient) -> None:
    resp = await gated_client.post(f"/gateways/sync?token={TOKEN}", json={"entries": []})
    assert resp.status_code == 200


async def test_gateways_sync_rejects_wrong_token(gated_client: AsyncClient) -> None:
    resp = await gated_client.post(
        "/gateways/sync", json={"entries": []}, headers={"X-CTTC-Token": "wrong"}
    )
    assert resp.status_code == 401


async def test_gateways_sync_unauthenticated_when_no_token_configured(client: AsyncClient) -> None:
    resp = await client.post("/gateways/sync", json={"entries": []})
    assert resp.status_code == 200


# ── /gateways/sync merge behavior ────────────────────────────────────────


async def test_gateways_sync_adds_self_entry(client: AsyncClient) -> None:
    resp = await client.post(
        "/gateways/sync", json={"entries": []}, headers={"Host": "10.0.0.9:8080"}
    )
    assert resp.status_code == 200
    entries = resp.json()["entries"]
    assert any(e["host"] == "10.0.0.9" and e["port"] == 8080 for e in entries)


async def test_gateways_sync_merges_incoming_entries_as_unknown(client: AsyncClient) -> None:
    resp = await client.post(
        "/gateways/sync",
        json={
            "entries": [
                {
                    "host": "10.0.0.5",
                    "port": 9090,
                    "existence": "existing",
                    "lastContactAt": "2026-08-14T00:00:00Z",
                }
            ]
        },
    )
    assert resp.status_code == 200
    entries = {f"{e['host']}:{e['port']}": e for e in resp.json()["entries"]}
    assert entries["10.0.0.5:9090"]["existence"] == "unknown"  # relayed, not locally verified


async def test_gateways_sync_uses_configured_public_address_over_host_header(
    redis: FakeAsyncRedis,
) -> None:
    app = create_app(
        settings=Settings(gateway=GatewayConfig(public_address="public.example:8080")),
        redis=redis,
    )
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/gateways/sync", json={"entries": []}, headers={"Host": "internal:1234"}
            )
    entries = resp.json()["entries"]
    assert any(e["host"] == "public.example" and e["port"] == 8080 for e in entries)
    assert not any(e["host"] == "internal" for e in entries)


# ── ownership claim ───────────────────────────────────────────────────────


async def test_ownership_claim_requires_label_and_public_key(client: AsyncClient) -> None:
    resp = await client.post(
        "/gateway/ownership/claim", json={"ownerLabel": "", "ownerPublicKey": ""}
    )
    assert resp.status_code == 422


async def test_ownership_claim_first_call_wins(client: AsyncClient) -> None:
    resp = await client.post(
        "/gateway/ownership/claim",
        json={"ownerLabel": "alice", "ownerPublicKey": "key-a"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ownerLabel"] == "alice"
    assert len(body["ownerKeyFingerprint"]) == 64


async def test_ownership_claim_is_idempotent(client: AsyncClient) -> None:
    first = await client.post(
        "/gateway/ownership/claim", json={"ownerLabel": "alice", "ownerPublicKey": "key-a"}
    )
    second = await client.post(
        "/gateway/ownership/claim", json={"ownerLabel": "bob", "ownerPublicKey": "key-b"}
    )
    assert second.status_code == 200
    assert second.json() == first.json()  # unchanged -- bob never actually claims it


# ── admin challenge + rotate ────────────────────────────────────────────


async def test_admin_challenge_issues_a_nonce(client: AsyncClient) -> None:
    resp = await client.get("/gateway/admin/challenge")
    assert resp.status_code == 200
    body = resp.json()
    assert body["nonce"]
    assert body["expiresInSeconds"] == 120.0


async def test_ownership_rotate_requires_a_claimed_owner(client: AsyncClient) -> None:
    resp = await client.post(
        "/gateway/ownership/rotate",
        json={
            "newOwnerLabel": "bob",
            "newOwnerPublicKey": "key-b",
            "nonce": "whatever",
            "signature": "whatever",
        },
    )
    assert resp.status_code == 403


def _generate_ed25519_keypair(tmp_path: Path) -> Path:
    key_path = tmp_path / "owner"
    subprocess.run(
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(key_path)],
        check=True,
        capture_output=True,
    )
    return key_path


def _sign_with_ssh_keygen(key_path: Path, data_file: Path, *, namespace: str) -> str:
    subprocess.run(
        ["ssh-keygen", "-Y", "sign", "-f", str(key_path), "-n", namespace, str(data_file)],
        check=True,
        capture_output=True,
    )
    return data_file.with_suffix(data_file.suffix + ".sig").read_text()


async def test_ownership_rotate_end_to_end_with_a_real_ssh_signature(
    client: AsyncClient, tmp_path: Path
) -> None:
    key_path = _generate_ed25519_keypair(tmp_path)
    owner_public_key = key_path.with_suffix(".pub").read_text()

    claim = await client.post(
        "/gateway/ownership/claim",
        json={"ownerLabel": "alice", "ownerPublicKey": owner_public_key},
    )
    assert claim.status_code == 200

    challenge = await client.get("/gateway/admin/challenge")
    nonce = challenge.json()["nonce"]

    nonce_file = tmp_path / "nonce.txt"
    nonce_file.write_text(nonce)
    signature = _sign_with_ssh_keygen(key_path, nonce_file, namespace="cttc-admin-auth")

    rotate = await client.post(
        "/gateway/ownership/rotate",
        json={
            "newOwnerLabel": "bob",
            "newOwnerPublicKey": "key-b",
            "nonce": nonce,
            "signature": signature,
        },
    )

    assert rotate.status_code == 200
    assert rotate.json()["ownerLabel"] == "bob"

    # the nonce is single-use -- retrying the exact same request must fail
    replay = await client.post(
        "/gateway/ownership/rotate",
        json={
            "newOwnerLabel": "mallory",
            "newOwnerPublicKey": "key-m",
            "nonce": nonce,
            "signature": signature,
        },
    )
    assert replay.status_code == 403


# ── /mlog ─────────────────────────────────────────────────────────────────


async def test_mlog_requires_token_when_configured(gated_client: AsyncClient) -> None:
    resp = await gated_client.get("/mlog")
    assert resp.status_code == 401


async def test_mlog_returns_a_fallback_message_outside_docker(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No live `docker ps` mocking here -- this sandbox may or may not have
    a real docker daemon reachable, so this only asserts the route wires
    gather_own_container_logs through correctly, not any particular
    container-discovery outcome. gather_own_container_logs.py's own tests
    (test_gateway_mesh.py) cover the found/not-found branches directly.
    """

    async def _fake_gather(timeout_s: float = 15.0) -> tuple[str, bytes]:
        return "test-gateway", b"log line 1\nlog line 2\n"

    monkeypatch.setattr(
        "log_sump.server.routers.gateway.gateway_mesh.gather_own_container_logs", _fake_gather
    )

    resp = await client.get("/mlog")

    assert resp.status_code == 200
    assert resp.headers["X-CTTC-Gateway-Name"] == "test-gateway"
    assert resp.content == b"log line 1\nlog line 2\n"


# ── /admin/redis-cli ────────────────────────────────────────────────────


async def test_admin_redis_cli_requires_token_when_configured(gated_client: AsyncClient) -> None:
    resp = await gated_client.post("/admin/redis-cli", json={"argv": ["PING"]})
    assert resp.status_code == 401


async def test_admin_redis_cli_runs_an_unrestricted_command(client: AsyncClient) -> None:
    resp = await client.post("/admin/redis-cli", json={"argv": ["SET", "k", "v"]})
    assert resp.status_code == 200
    assert resp.json() == {"type": "status", "value": "OK"}


async def test_admin_redis_cli_returns_a_typed_error_for_a_bad_command(client: AsyncClient) -> None:
    resp = await client.post("/admin/redis-cli", json={"argv": ["NOTACOMMAND"]})
    assert resp.status_code == 200
    assert resp.json()["type"] == "error"


# ── /shutdown ────────────────────────────────────────────────────────────


class _FakeUvicornServer:
    def __init__(self) -> None:
        self.should_exit = False


async def test_shutdown_requires_token_when_configured(gated_client: AsyncClient) -> None:
    resp = await gated_client.post("/shutdown")
    assert resp.status_code == 401


async def test_shutdown_sets_should_exit_on_the_uvicorn_server(redis: FakeAsyncRedis) -> None:
    app = create_app(settings=Settings(), redis=redis)
    async with app.router.lifespan_context(app):
        fake_server = _FakeUvicornServer()
        app.state.uvicorn_server = fake_server
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/shutdown")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        await asyncio.sleep(0.2)
        assert fake_server.should_exit is True
