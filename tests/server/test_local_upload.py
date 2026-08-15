"""Migration plan Phase 2: importing a plain log file or a .cttc-metric
archive with no daemon/container involved.
"""

import io
import json
import zipfile

from fakeredis import FakeAsyncRedis

from log_sump.common.redis_keys import auth_key, stream_key
from log_sump.common.schema import Kind, LogRecord, MetricRecord, RecordAdapter
from log_sump.server.local_upload import ingest_upload

API_KEY = "uploader-key"


def _archive_bytes() -> bytes:
    log_rows = [{"ts": 1000.0, "text": "boot"}, {"ts": 2000.0, "text": "ready"}]
    stats_payload = {"series": {"web": [[1000.0, 10.0, 20.0, 1024, 500.0]]}, "swarm": []}
    manifest = {
        "version": 3,
        "segments": [
            {
                "from": 1000.0,
                "to": 2000.0,
                "created": "2026-08-14T12:00:00Z",
                "sources": [
                    {"type": "log", "name": "web", "file": "seg0/logs/0.jsonl", "count": 2},
                    {"type": "stats", "name": "web", "file": "seg0/stats/0.json", "is_host": False},
                ],
            }
        ],
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", json.dumps(manifest))
        z.writestr("seg0/logs/0.jsonl", "\n".join(json.dumps(r) for r in log_rows))
        z.writestr("seg0/stats/0.json", json.dumps(stats_payload))
    return buf.getvalue()


async def test_ingest_upload_plain_text_file() -> None:
    redis = FakeAsyncRedis()
    data = b"2026-08-14T12:00:00.000000000Z hello world\n"

    result = await ingest_upload(redis, filename="app.log", data=data, api_key=API_KEY)

    assert result.log_count == 1
    assert result.metric_count == 0
    entries = await redis.xrange(stream_key(result.docker_host, Kind.LOG))
    assert entries is not None
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields is not None
    record = RecordAdapter.validate_json(fields[b"data"])
    assert isinstance(record, LogRecord)
    assert record.message == "hello world"


async def test_ingest_upload_grants_uploader_access_to_synthetic_host() -> None:
    redis = FakeAsyncRedis()
    data = b"2026-08-14T12:00:00.000000000Z hello\n"

    result = await ingest_upload(redis, filename="app.log", data=data, api_key=API_KEY)

    permitted = await redis.smembers(auth_key(API_KEY))
    assert result.docker_host.encode() in permitted or result.docker_host in permitted


async def test_ingest_upload_sample_archive_produces_logs_and_metrics() -> None:
    redis = FakeAsyncRedis()

    result = await ingest_upload(
        redis, filename="sample.cttc-metric", data=_archive_bytes(), api_key=API_KEY
    )

    assert result.log_count == 2
    assert result.metric_count == 1
    log_entries = await redis.xrange(stream_key(result.docker_host, Kind.LOG))
    metric_entries = await redis.xrange(stream_key(result.docker_host, Kind.METRIC))
    assert log_entries is not None
    assert len(log_entries) == 2
    assert metric_entries is not None
    assert len(metric_entries) == 1
    metric_fields = metric_entries[0][1]
    assert metric_fields is not None
    metric = RecordAdapter.validate_json(metric_fields[b"data"])
    assert isinstance(metric, MetricRecord)
    assert metric.cpu_pct == 10.0
    assert metric.raw == {"imported_net_rate_bps": 500.0}  # rate preserved, not re-derived


async def test_ingest_upload_stream_ids_reflect_historical_timestamps() -> None:
    """The whole point of explicit ids in _bulk_write: entries must be
    queryable by their own historical time range, not by "whenever the
    upload happened to run".
    """
    redis = FakeAsyncRedis()
    result = await ingest_upload(
        redis, filename="sample.cttc-metric", data=_archive_bytes(), api_key=API_KEY
    )

    entries = await redis.xrange(
        stream_key(result.docker_host, Kind.LOG), min="1000-0", max="2000-999"
    )
    assert entries is not None and len(entries) == 2


async def test_ingest_upload_is_idempotent_on_identical_reupload() -> None:
    redis = FakeAsyncRedis()
    data = _archive_bytes()

    first = await ingest_upload(redis, filename="sample.cttc-metric", data=data, api_key=API_KEY)
    second = await ingest_upload(redis, filename="sample.cttc-metric", data=data, api_key=API_KEY)

    assert first.docker_host == second.docker_host
    entries = await redis.xrange(stream_key(first.docker_host, Kind.LOG))
    assert entries is not None and len(entries) == 2  # not duplicated
