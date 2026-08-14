"""The one place a validated Record actually becomes an `XADD` -- shared by
`ingest/consumer.py` (the Logstash-fed live path) and `local_upload.py`
(migration plan Phase 2's upload/import path), so "one record -> one XADD
into stream_key(docker_host, kind)" stays enforced in exactly one place
regardless of which path a record arrived through.
"""

from __future__ import annotations

from typing import Any

from log_sump_common.redis_keys import stream_key
from log_sump_common.schema import LogRecord, MetricRecord, RecordAdapter, ServiceRecord


def queue_record(
    pipe: Any, record: LogRecord | MetricRecord | ServiceRecord, *, entry_id: str | None = None
) -> None:
    """Queue one record's `XADD` onto an already-open Redis pipeline.

    `entry_id`, when given, is an explicit `<ms>-<seq>` Stream ID instead of
    letting Redis auto-assign one from wall-clock time -- required for
    *historical* bulk import (Phase 2): auto-assigned IDs track real
    ingestion time, which is fine for the Logstash-fed live path (arrival
    time ~= event time) but wrong for importing an old sample, where every
    query in `queries.py` needs each entry's Stream ID to actually reflect
    the *record's own* `ts`, not "whenever this upload happened to run".
    """
    stream = stream_key(record.docker_host, record.kind)
    data = {"data": RecordAdapter.dump_json(record)}
    if entry_id is None:
        pipe.xadd(stream, data)
    else:
        pipe.xadd(stream, data, id=entry_id)
