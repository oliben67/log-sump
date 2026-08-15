import asyncio
import contextlib
import json

from fakeredis import FakeAsyncRedis

from log_sump.common.redis_keys import INGEST_LIST, stream_key
from log_sump.common.schema import Kind, LogRecord, RecordAdapter
from log_sump.server.broadcast import Broadcaster
from log_sump.server.ingest.consumer import run_consumer

LOG_RECORD_JSON = json.dumps(
    {
        "kind": "log",
        "docker_host": "daemon-a",
        "container_name": "web",
        "container_id": "c1",
        "ts": "2026-08-14T12:00:00Z",
        "seq": 1,
        "stream": "stdout",
        "level": "info",
        "message": "hello",
        "fields": {},
        "raw": "hello",
    }
)

METRIC_RECORD_JSON = json.dumps(
    {
        "kind": "metric",
        "docker_host": "daemon-a",
        "container_name": "web",
        "container_id": "c1",
        "ts": "2026-08-14T12:00:01Z",
        "seq": 1,
        "metric_scope": "container",
        "source": "docker stats",
    }
)


async def _consume_and_capture(
    redis, stream_keys: list[str], **run_consumer_kwargs
) -> dict[str, list]:
    """Run the consumer until the ingest list drains, then snapshot the
    given streams -- all *before* cancelling the background task.

    Reading fakeredis state after cancelling a task that was blocked inside
    BLPOP loses the entire in-memory dataset (a fakeredis quirk: confirmed
    it wipes even unrelated keys, not something about our own code) -- so
    every read this test cares about has to happen while the task is still
    alive, and cancellation has to be the last thing that touches `redis`.
    """
    task = asyncio.create_task(run_consumer(redis, **run_consumer_kwargs))
    try:
        async with asyncio.timeout(2.0):
            # Polling external Redis state, not our own code's state -- an
            # asyncio.Event has no producer to set() here.
            while await redis.llen(INGEST_LIST) != 0:  # noqa: ASYNC110
                await asyncio.sleep(0.02)
            return {key: await redis.xrange(key) for key in stream_keys}
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_consumer_routes_log_and_metric_records_to_separate_streams() -> None:
    redis = FakeAsyncRedis()
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON, METRIC_RECORD_JSON)

    streams = await _consume_and_capture(
        redis,
        [stream_key("daemon-a", Kind.LOG), stream_key("daemon-a", Kind.METRIC)],
        poll_timeout_s=0.05,
    )

    log_entries = streams[stream_key("daemon-a", Kind.LOG)]
    metric_entries = streams[stream_key("daemon-a", Kind.METRIC)]
    assert len(log_entries) == 1
    assert len(metric_entries) == 1

    _id, fields = log_entries[0]
    record = RecordAdapter.validate_json(fields[b"data"])
    assert record.kind == Kind.LOG
    assert record.message == "hello"


async def test_consumer_drops_malformed_entries_without_crashing() -> None:
    redis = FakeAsyncRedis()
    await redis.rpush(INGEST_LIST, "not valid json", LOG_RECORD_JSON)

    streams = await _consume_and_capture(
        redis, [stream_key("daemon-a", Kind.LOG)], poll_timeout_s=0.05
    )

    assert (
        len(streams[stream_key("daemon-a", Kind.LOG)]) == 1
    )  # malformed entry dropped, not stored


async def test_consumer_uses_auto_generated_stream_ids() -> None:
    redis = FakeAsyncRedis()
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON)

    streams = await _consume_and_capture(
        redis, [stream_key("daemon-a", Kind.LOG)], poll_timeout_s=0.05
    )

    stream_id = streams[stream_key("daemon-a", Kind.LOG)][0][0]
    # Redis auto IDs look like "<ms>-<seq>", not the record's own `ts`/`seq`.
    assert b"-" in stream_id


async def test_consumer_applies_transform_fns_to_log_records_only() -> None:
    def uppercase(record: dict) -> dict:
        record = dict(record)
        record["message"] = record["message"].upper()
        return record

    redis = FakeAsyncRedis()
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON, METRIC_RECORD_JSON)

    streams = await _consume_and_capture(
        redis,
        [stream_key("daemon-a", Kind.LOG), stream_key("daemon-a", Kind.METRIC)],
        poll_timeout_s=0.05,
        transform_fns=[("uppercase", uppercase)],
    )

    log_fields = streams[stream_key("daemon-a", Kind.LOG)][0][1]
    assert log_fields is not None
    log_record = RecordAdapter.validate_json(log_fields[b"data"])
    assert isinstance(log_record, LogRecord)
    assert log_record.message == "HELLO"  # transformed
    metric_entries = streams[stream_key("daemon-a", Kind.METRIC)]
    assert len(metric_entries) == 1  # untouched -- transforms are log-only, matching cttc


async def test_consumer_publishes_one_update_event_per_touched_docker_host() -> None:
    redis = FakeAsyncRedis()
    broadcaster = Broadcaster()
    queue = broadcaster.subscribe()
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON, METRIC_RECORD_JSON)

    await _consume_and_capture(
        redis,
        [stream_key("daemon-a", Kind.LOG), stream_key("daemon-a", Kind.METRIC)],
        poll_timeout_s=0.05,
        broadcaster=broadcaster,
    )

    # Both records share docker_host="daemon-a" -- exactly one notification,
    # not two, even though two separate streams received data.
    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert event == {"type": "update", "docker_host": "daemon-a"}
    assert queue.empty()


async def test_consumer_without_a_broadcaster_does_not_raise() -> None:
    redis = FakeAsyncRedis()
    await redis.rpush(INGEST_LIST, LOG_RECORD_JSON)

    await _consume_and_capture(redis, [stream_key("daemon-a", Kind.LOG)], poll_timeout_s=0.05)
