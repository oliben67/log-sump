from datetime import UTC, datetime

from log_sump.common.log_line_parsing import parse_log_lines, parse_ts


def test_parse_ts_handles_docker_nanosecond_timestamps() -> None:
    ms = parse_ts("2026-08-14T12:00:00.123456789Z")
    assert ms is not None
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    assert dt.year == 2026 and dt.month == 8 and dt.day == 14
    assert dt.microsecond == 123456


def test_parse_ts_naive_timestamp_treated_as_utc() -> None:
    ms = parse_ts("2026-08-14 12:00:00")
    assert ms == parse_ts("2026-08-14T12:00:00Z")


def test_parse_ts_rejects_non_timestamp_text() -> None:
    assert parse_ts("hello world") is None


def test_parse_log_lines_strips_docker_timestamp_prefix() -> None:
    rows = parse_log_lines("2026-08-14T12:00:00.000000000Z hello world\n")
    assert len(rows) == 1
    assert rows[0].text == "hello world"


def test_parse_log_lines_strips_service_log_prefix() -> None:
    rows = parse_log_lines(
        "2026-08-14T12:00:00.000000000Z web.1.abc123@node1    | actual message\n"
    )
    assert len(rows) == 1
    assert rows[0].text == "actual message"


def test_parse_log_lines_json_body_supplies_own_timestamp() -> None:
    rows = parse_log_lines('{"ts": "2026-08-14T12:00:00Z", "msg": "hi"}\n')
    assert len(rows) == 1
    assert rows[0].fields == {"ts": "2026-08-14T12:00:00Z", "msg": "hi"}


def test_parse_log_lines_json_body_epoch_seconds_timestamp() -> None:
    rows = parse_log_lines('{"timestamp": 1786737600, "msg": "hi"}\n')
    assert len(rows) == 1
    assert rows[0].ts_ms == 1786737600 * 1000.0


def test_parse_log_lines_continuation_line_appends_to_previous_row() -> None:
    text = (
        "2026-08-14T12:00:00.000000000Z Traceback (most recent call last):\n"
        "  File \"x.py\", line 1\n"
        "    raise ValueError()\n"
    )
    rows = parse_log_lines(text)
    assert len(rows) == 1
    assert "Traceback" in rows[0].text
    assert 'File "x.py"' in rows[0].text
    assert "raise ValueError()" in rows[0].text


def test_parse_log_lines_continuation_with_no_prior_row_is_dropped() -> None:
    rows = parse_log_lines("not a timestamped line at all\n")
    assert rows == []


def test_parse_log_lines_skips_blank_lines() -> None:
    text = "2026-08-14T12:00:00.000000000Z first\n\n2026-08-14T12:00:01.000000000Z second\n"
    rows = parse_log_lines(text)
    assert [r.text for r in rows] == ["first", "second"]
