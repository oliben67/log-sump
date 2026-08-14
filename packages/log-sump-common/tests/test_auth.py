from fakeredis import FakeAsyncRedis
from log_sump_common.auth import RedisApiKeyAuthBackend
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
