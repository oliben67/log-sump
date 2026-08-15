"""Local/static file ingestion (migration plan Phase 2): imports an
uploaded log file or `.cttc-metric`/`.cttc-record` archive with no
daemon/container involved at all -- today's `LogSource`/`StatsSource`
static-file path and `/open` route's replacement.

No live daemon backs this data, so there's no pre-provisioned
`docker_host` to scope it against the way a configured daemon's key is
(`config.example.yaml`'s `daemons:` list, checked via `auth.py`'s
`RedisApiKeyAuthBackend`). Instead a `docker_host` is synthesized,
content-addressed from the uploaded bytes themselves
(`local:<sha256(data)[:16]>`), so re-uploading the identical file twice
resolves to the identical id -- cttc's own `_content_entity_id`'s
idempotent-reload property (`server.py`'s docstring: "re-loading the exact
same recording/metric twice resolves to the same entity"), just derived
from raw content instead of gateway/host/container provenance. The
uploading caller's own API key is then granted access to that id
(`auth_key`, the same Redis set `/catalog`/`/records` already read), so the
client that just uploaded a file can immediately query it through every
ordinary daemon-scoped endpoint (`/records`, `/point`, `/series`, ...) --
no separate "local file" query surface needed.

Provisioning note: `RedisApiKeyAuthBackend.permitted_daemons` treats a key
that maps to an *empty* permitted set the same as an unknown key (see
`auth.py`) -- an upload-only key that has never been granted a real daemon
still needs *some* provisioned entry before its very first upload, or it
401s like any other unrecognized key. Grant such a key one placeholder
value (anything -- it's never checked against a real daemon) to mark it as
known.

Bulk-writes with *explicit* Stream IDs derived from each record's own `ts`
(see `write.queue_record`'s `entry_id` param) rather than letting Redis
auto-assign from wall-clock time: this data's `ts` values are historical,
not "now", and every query in `queries.py` assumes a Stream ID reflects the
record's own event time.
"""

from __future__ import annotations

import hashlib
import itertools
from collections.abc import Sequence
from datetime import UTC, datetime

from log_sump_common.cttc_archive import ArchivedSource, is_cttc_archive, read_archive
from log_sump_common.log_line_parsing import parse_log_lines
from log_sump_common.redis_keys import auth_key, stream_key
from log_sump_common.schema import Kind, LogRecord, MetricRecord
from redis.asyncio import Redis

from .broadcast import Broadcaster
from .ingest.write import queue_record


class UploadResult:
    def __init__(self, docker_host: str, log_count: int, metric_count: int) -> None:
        self.docker_host = docker_host
        self.log_count = log_count
        self.metric_count = metric_count


def _synthetic_docker_host(data: bytes) -> str:
    return f"local:{hashlib.sha256(data).hexdigest()[:16]}"


async def ingest_upload(
    redis: Redis,
    *,
    filename: str,
    data: bytes,
    api_key: str,
    segment: int | None = None,
    broadcaster: Broadcaster | None = None,
) -> UploadResult:
    """Parse `data` (dispatched on `filename`'s extension, like cttc's own
    `files.upload_and_open`) into Records, bulk-`XADD`s them under a
    content-addressed synthetic `docker_host`, grants `api_key` access to
    it, and reports what was ingested. `broadcaster`, if given, publishes
    an SSE `{"type": "update", "docker_host": ...}` notification (migration
    plan Phase 6) once the data actually lands.

    Raises `log_sump_common.cttc_archive.MultiSegmentArchive` unchanged --
    same "ask the caller which segment" contract as cttc's own
    `MultiSegmentSample`.
    """
    docker_host = _synthetic_docker_host(data)

    logs: list[LogRecord] = []
    metrics: list[MetricRecord] = []
    if is_cttc_archive(filename):
        for source in read_archive(data, segment=segment):
            logs.extend(_archived_log_records(docker_host, source))
            metrics.extend(_archived_metric_records(docker_host, source))
    else:
        logs.extend(_plain_log_records(docker_host, filename, data))

    await _bulk_write(redis, docker_host, Kind.LOG, logs)
    await _bulk_write(redis, docker_host, Kind.METRIC, metrics)
    if logs or metrics:
        await redis.sadd(auth_key(api_key), docker_host)
        if broadcaster is not None:
            broadcaster.publish({"type": "update", "docker_host": docker_host})
    return UploadResult(docker_host=docker_host, log_count=len(logs), metric_count=len(metrics))


def _plain_log_records(docker_host: str, filename: str, data: bytes) -> list[LogRecord]:
    container = filename or "upload"
    seq = itertools.count(1)
    return [
        LogRecord(
            docker_host=docker_host,
            container_name=container,
            container_id=container,
            ts=datetime.fromtimestamp(row.ts_ms / 1000, tz=UTC),
            seq=next(seq),
            stream="stdout",
            level="info",
            message=row.text,
            fields=row.fields,
            raw=row.text,
        )
        for row in parse_log_lines(data.decode("utf-8", errors="replace"))
    ]


def _archived_log_records(docker_host: str, source: ArchivedSource) -> list[LogRecord]:
    if source.kind != "log":
        return []
    seq = itertools.count(1)
    return [
        LogRecord(
            docker_host=docker_host,
            container_name=source.name,
            container_id=source.name,
            ts=datetime.fromtimestamp(row.ts_ms / 1000, tz=UTC),
            seq=next(seq),
            stream="stdout",
            level="info",
            message=row.text,
            fields={},
            raw=row.text,
        )
        for row in source.log_rows
    ]


def _archived_metric_records(docker_host: str, source: ArchivedSource) -> list[MetricRecord]:
    if source.kind != "stats":
        return []
    out: list[MetricRecord] = []
    for svc, rows in source.stats_series.items():
        # A dotted name round-trips cttc's own swarm-service grouping
        # through log-sump's own query-time _metric_group (see
        # ArchivedSource's docstring) -- a plain container has no dot and
        # stays its own group either way.
        container_name = f"{svc}.imported.0" if svc in source.swarm_services else svc
        seq = itertools.count(1)
        for ts_ms, cpu, mem, mem_bytes, net in rows:
            out.append(
                MetricRecord(
                    docker_host=docker_host,
                    container_name=container_name,
                    container_id=container_name,
                    ts=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
                    seq=next(seq),
                    metric_scope="container",
                    cpu_pct=cpu,
                    mem_pct=mem,
                    mem_used_bytes=int(mem_bytes) if mem_bytes is not None else None,
                    source="cttc-import",
                    # cttc's archive stores an already-computed rate, not a
                    # cumulative counter -- can't fill net_rx_bytes/
                    # net_tx_bytes without fabricating a split that was
                    # never recorded, so the original value is preserved
                    # verbatim here instead (see ArchivedSource's own
                    # stats_series docstring).
                    raw={"imported_net_rate_bps": net} if net is not None else None,
                )
            )
    return out


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _id_tuple(entry_id: bytes | str) -> tuple[int, int]:
    ms_s, seq_s = _decode(entry_id).split("-", 1)
    return int(ms_s), int(seq_s)


async def _bulk_write(
    redis: Redis, docker_host: str, kind: Kind, records: Sequence[LogRecord | MetricRecord]
) -> None:
    if not records:
        return
    records = sorted(records, key=lambda r: r.ts)
    stream = stream_key(docker_host, kind)
    first_id = (int(records[0].ts.timestamp() * 1000), 0)

    existing_top = await redis.xrevrange(stream, count=1)
    top_id = existing_top[0][0] if existing_top else None
    if top_id is not None and _id_tuple(top_id) >= first_id:
        # Idempotent re-upload of the same content (docker_host is content-
        # addressed): this data is already there. Redis would reject
        # writing these same ids again anyway (XADD requires strictly
        # increasing ids per stream) -- skip rather than let that surface
        # as an error.
        return

    pipe = redis.pipeline(transaction=False)
    for i, record in enumerate(records):
        entry_id = f"{int(record.ts.timestamp() * 1000)}-{i}"
        queue_record(pipe, record, entry_id=entry_id)
    await pipe.execute()
