"""POST /events/create etc -- migration plan Phase 5. Router-level auth
scoping and request parsing; the actual watch/fire logic is covered in
test_events.py.
"""

from collections.abc import AsyncIterator

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from log_sump_common.config import Settings
from log_sump_common.redis_keys import auth_key
from log_sump_server.app import create_app

API_KEY = "events-key"


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    await client.sadd(auth_key(API_KEY), "daemon-a")
    yield client


@pytest.fixture
async def client(redis: FakeAsyncRedis) -> AsyncIterator[AsyncClient]:
    app = create_app(settings=Settings(), redis=redis)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


def _create_body(**overrides: object) -> dict:
    body = {
        "docker_host": "daemon-a",
        "name": "cpu high",
        "conditions": [{"type": "metric", "metric": "cpu", "op": ">", "threshold": 80}],
        "action": {"kind": "snapshot", "minutes": 5},
    }
    body.update(overrides)
    return body


async def test_create_event_requires_daemon_access(client: AsyncClient) -> None:
    resp = await client.post(
        "/events/create", json=_create_body(docker_host="daemon-b"), headers=_auth_headers()
    )
    assert resp.status_code == 403


async def test_create_rejects_a_condition_pydantic_cant_catch(client: AsyncClient) -> None:
    # metric/op are Literal fields, so a genuinely unknown value there 422s
    # at the request-parsing boundary before ever reaching _validate() --
    # an invalid regex is a plain string field, so it's the case that
    # actually exercises _validate()'s own InvalidEvent -> 400 path.
    resp = await client.post(
        "/events/create",
        json=_create_body(conditions=[{"type": "log", "pattern": "["}]),
        headers=_auth_headers(),
    )
    assert resp.status_code == 400


async def test_create_rejects_unknown_metric_at_the_request_boundary(client: AsyncClient) -> None:
    resp = await client.post(
        "/events/create",
        json=_create_body(
            conditions=[{"type": "metric", "metric": "disk", "op": ">", "threshold": 1}]
        ),
        headers=_auth_headers(),
    )
    assert resp.status_code == 422


async def test_create_list_status_and_cancel_event(client: AsyncClient) -> None:
    create_resp = await client.post(
        "/events/create", json=_create_body(), headers=_auth_headers()
    )
    assert create_resp.status_code == 200
    event_id = create_resp.json()["event_id"]

    list_resp = await client.get("/events/list", headers=_auth_headers())
    assert event_id in list_resp.json()["event_ids"]

    status_resp = await client.get(f"/events/{event_id}", headers=_auth_headers())
    assert status_resp.status_code == 200
    assert status_resp.json()["docker_host"] == "daemon-a"

    disable_resp = await client.post(f"/events/{event_id}/disable", headers=_auth_headers())
    assert disable_resp.status_code == 200
    disabled = await client.get(f"/events/{event_id}", headers=_auth_headers())
    assert disabled.json()["enabled"] is False

    enable_resp = await client.post(f"/events/{event_id}/enable", headers=_auth_headers())
    assert enable_resp.status_code == 200

    reset_resp = await client.post(f"/events/{event_id}/reset", headers=_auth_headers())
    assert reset_resp.status_code == 200

    update_resp = await client.post(
        f"/events/{event_id}/update", json={"name": "renamed"}, headers=_auth_headers()
    )
    assert update_resp.status_code == 200
    renamed = await client.get(f"/events/{event_id}", headers=_auth_headers())
    assert renamed.json()["name"] == "renamed"

    cancel_resp = await client.post(f"/events/{event_id}/cancel", headers=_auth_headers())
    assert cancel_resp.status_code == 200
    assert (await client.get(f"/events/{event_id}", headers=_auth_headers())).status_code == 404


async def test_event_actions_require_api_key(client: AsyncClient) -> None:
    resp = await client.post("/events/create", json=_create_body())
    assert resp.status_code == 401
