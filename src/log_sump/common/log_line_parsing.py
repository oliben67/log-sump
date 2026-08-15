"""Timestamp/line parsing for arbitrary local log files -- migration plan
Phase 2 (the "open a local file with no daemon/container involved" gap).

Ported from cttc's own `server.py` (`parse_ts`/`LogSource._parse_line`/
`LogSource.ingest_chunk`'s continuation-line handling) unchanged: same ISO
timestamp regex (handles docker's 9-digit nanosecond fractions), same
`docker service logs`-prefix strip, same JSON-body-with-embedded-timestamp
fallback, same "no timestamp of its own -> append to the previous row"
continuation-line rule for multi-line entries like stack traces.

Dependency-free (no Redis/FastAPI/State ties), matching `cttc_archive.py`'s
own reasoning for living in `log_sump.common` rather than `log_sump.server`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})"
    r"(?:[.,](\d{1,9}))?\s*(Z|[+-]\d{2}:?\d{2})?"
)


def parse_ts(text: str) -> float | None:
    """ISO-ish timestamp -> epoch ms. Handles docker's 9-digit nanoseconds.
    A bare (no offset) timestamp is treated as UTC.
    """
    m = ISO_RE.match(text.strip())
    if not m:
        return None
    y, mo, d, h, mi, s = (int(m.group(i)) for i in range(1, 7))
    frac = m.group(7)
    us = int(frac.ljust(6, "0")[:6]) if frac else 0
    off = m.group(8)
    if off is None:
        tz = UTC
    elif off in ("Z", "z"):
        tz = UTC
    else:
        sign = 1 if off[0] == "+" else -1
        hh, mm = int(off[1:3]), int(off[-2:])
        tz = timezone(sign * timedelta(hours=hh, minutes=mm))
    try:
        dt = datetime(y, mo, d, h, mi, s, us, tz)
    except ValueError:
        return None
    return dt.timestamp() * 1000.0


#: `docker service logs`'s own `<task>.<n>.<id>@<node> | ` line prefix.
DOCKER_SVCLOG_PREFIX = re.compile(r"^(\S+\.\d+\.\S+@\S+|\S+)\s+\|\s?")
TS_FIELDS = ("timestamp", "ts", "time", "@timestamp", "datetime", "date")


@dataclass(frozen=True)
class ParsedLine:
    ts_ms: float
    text: str
    fields: dict


def _parse_line(raw: str) -> ParsedLine | None:
    """-> a parsed row, or `None` when the line has no timestamp of its own
    (a continuation line, per `parse_log_lines`'s docstring).
    """
    text = raw
    fields: dict = {}
    sp = raw.split(" ", 1)
    ts = parse_ts(sp[0])
    if ts is not None:
        text = sp[1] if len(sp) > 1 else ""
        text = DOCKER_SVCLOG_PREFIX.sub("", text, count=1)
    body = text.lstrip()
    if body.startswith("{") and body.endswith("}"):
        try:
            parsed = json.loads(body)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            fields = parsed
            if ts is None:
                for f in TS_FIELDS:
                    v = fields.get(f)
                    if isinstance(v, str):
                        ts = parse_ts(v)
                    elif isinstance(v, int | float):
                        ts = float(v) * (1000.0 if v < 1e12 else 1.0)
                    if ts is not None:
                        break
    if ts is None:
        return None
    return ParsedLine(ts_ms=ts, text=text, fields=fields)


def parse_log_lines(text: str) -> list[ParsedLine]:
    """A whole uploaded file's worth of lines -> parsed rows.

    A line with no timestamp of its own (a stack trace's continuation
    lines, for example) is appended -- newline-joined -- onto the previous
    row's `text` rather than becoming its own row or being dropped, unless
    there is no previous row yet, in which case it's skipped (matches
    cttc's `LogSource.ingest_chunk`).
    """
    rows: list[ParsedLine] = []
    for raw in text.splitlines():
        stripped_raw = raw.rstrip("\r")
        if not stripped_raw.strip():
            continue
        parsed = _parse_line(stripped_raw)
        if parsed is None:
            if rows:
                prev = rows[-1]
                rows[-1] = ParsedLine(
                    ts_ms=prev.ts_ms, text=prev.text + "\n" + stripped_raw, fields=prev.fields
                )
            continue
        rows.append(parsed)
    return rows
