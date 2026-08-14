import asyncio

from log_sump_common.schema import LogRecord, RecordAdapter
from log_sump_common.transport import StreamLine
from log_sump_listener.container_listener import _parse_line, run_container_listener

from .conftest import FakeRecordsLogger, FakeTransport


def test_parse_line_plain_text_uses_default_level_and_raw_as_message() -> None:
    line = StreamLine(stream="stdout", text="2026-08-14T12:00:00.123456789Z hello world")
    record = _parse_line(docker_host="d", container_id="c1", container_name="web", line=line, seq=1)

    assert record is not None
    assert record.level == "info"
    assert record.message == "hello world"
    assert record.fields == {}
    assert record.raw == line.text
    assert record.stream == "stdout"
    assert record.seq == 1


def test_parse_line_json_payload_extracts_level_message_and_fields() -> None:
    text = '2026-08-14T12:00:00Z {"level":"error","message":"boom","trace_id":"xyz"}'
    line = StreamLine(stream="stderr", text=text)
    record = _parse_line(docker_host="d", container_id="c1", container_name="web", line=line, seq=2)

    assert record is not None
    assert record.level == "error"
    assert record.message == "boom"
    assert record.fields == {"trace_id": "xyz"}
    assert record.stream == "stderr"


def test_parse_line_non_object_json_is_treated_as_plain_text() -> None:
    line = StreamLine(stream="stdout", text='2026-08-14T12:00:00Z ["not", "an", "object"]')
    record = _parse_line(docker_host="d", container_id="c1", container_name="web", line=line, seq=1)

    assert record is not None
    assert record.message == '["not", "an", "object"]'
    assert record.fields == {}


def test_parse_line_without_valid_timestamp_returns_none() -> None:
    line = StreamLine(stream="stdout", text="not-a-timestamp hello")
    result = _parse_line(docker_host="d", container_id="c1", container_name="web", line=line, seq=1)

    assert result is None


async def test_run_container_listener_emits_records_with_incrementing_seq_across_streams() -> None:
    lines = [
        StreamLine(stream="stdout", text='2026-08-14T12:00:00Z {"level":"info","message":"one"}'),
        StreamLine(stream="stderr", text="2026-08-14T12:00:01Z two"),
    ]
    transport = FakeTransport(lines)
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_container_listener("daemon-a", "c1", "web", transport, records_logger)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(records_logger.calls) == 2
    first = RecordAdapter.validate_json(records_logger.calls[0])
    second = RecordAdapter.validate_json(records_logger.calls[1])
    assert isinstance(first, LogRecord)
    assert isinstance(second, LogRecord)
    assert (first.message, first.seq) == ("one", 1)
    assert (second.message, second.seq, second.stream) == ("two", 2, "stderr")
