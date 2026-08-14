from fakeredis import FakeAsyncRedis
from log_sump_common.config import DaemonConfig
from log_sump_common.daemon_registry import (
    list_registered_daemons,
    register_daemon,
    unregister_daemon,
)


async def test_register_and_list_daemon() -> None:
    redis = FakeAsyncRedis()
    daemon = DaemonConfig(id="prod-a", host="10.0.0.5", user="deploy", transport="ssh")

    await register_daemon(redis, daemon)
    listed = await list_registered_daemons(redis)

    assert len(listed) == 1
    assert listed[0].id == "prod-a"
    assert listed[0].host == "10.0.0.5"


async def test_list_registered_daemons_empty_by_default() -> None:
    redis = FakeAsyncRedis()
    assert await list_registered_daemons(redis) == []


async def test_register_daemon_overwrites_same_id() -> None:
    redis = FakeAsyncRedis()
    await register_daemon(redis, DaemonConfig(id="a", host="1.1.1.1"))
    await register_daemon(redis, DaemonConfig(id="a", host="2.2.2.2"))

    listed = await list_registered_daemons(redis)

    assert len(listed) == 1
    assert listed[0].host == "2.2.2.2"


async def test_unregister_daemon_returns_true_when_present() -> None:
    redis = FakeAsyncRedis()
    await register_daemon(redis, DaemonConfig(id="a", host="1.1.1.1"))

    removed = await unregister_daemon(redis, "a")

    assert removed is True
    assert await list_registered_daemons(redis) == []


async def test_unregister_daemon_returns_false_when_absent() -> None:
    redis = FakeAsyncRedis()
    assert await unregister_daemon(redis, "nonexistent") is False
