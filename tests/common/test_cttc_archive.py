"""Reader tests for cttc's own .cttc-metric/.cttc-record archive format --
each fixture here is hand-built with the exact zip+manifest shape cttc's
server.py (_write_segment/build_sample_bytes/merge_sample_bytes) produces,
not round-tripped through log-sump's own writer (there isn't one -- see
cttc_archive.py's module docstring).
"""

import io
import json
import zipfile

import pytest

from log_sump.common.cttc_archive import (
    MultiSegmentArchive,
    is_cttc_archive,
    read_archive,
)


def _zip_with(manifest: dict, files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", json.dumps(manifest))
        for name, content in files.items():
            z.writestr(name, content)
    return buf.getvalue()


def _log_source(name: str, file: str, rows: list[dict]) -> dict:
    return {"type": "log", "name": name, "file": file, "count": len(rows)}


def _stats_source(name: str, file: str) -> dict:
    return {"type": "stats", "name": name, "file": file, "is_host": False}


def test_is_cttc_archive_recognizes_both_extensions() -> None:
    assert is_cttc_archive("sample.cttc-metric")
    assert is_cttc_archive("recording.cttc-record")
    assert not is_cttc_archive("plain.log")


def test_read_archive_single_segment_log_source() -> None:
    log_rows = [{"ts": 1000.0, "text": "hello"}, {"ts": 2000.0, "text": "world"}]
    manifest = {
        "version": 3,
        "segments": [
            {
                "from": 1000.0,
                "to": 2000.0,
                "created": "2026-08-14T12:00:00Z",
                "sources": [_log_source("web", "seg0/logs/0.jsonl", log_rows)],
            }
        ],
    }
    files = {"seg0/logs/0.jsonl": "\n".join(json.dumps(r) for r in log_rows).encode()}
    data = _zip_with(manifest, files)

    sources = read_archive(data)

    assert len(sources) == 1
    assert sources[0].kind == "log"
    assert sources[0].name == "web"
    assert [r.text for r in sources[0].log_rows] == ["hello", "world"]
    assert [r.ts_ms for r in sources[0].log_rows] == [1000.0, 2000.0]


def test_read_archive_stats_source_with_swarm_marker() -> None:
    payload = {"series": {"web": [[1000.0, 12.5, 30.0, 1024, 500.0]]}, "swarm": ["web"]}
    manifest = {
        "version": 3,
        "segments": [
            {
                "from": 1000.0,
                "to": 1000.0,
                "created": "2026-08-14T12:00:00Z",
                "sources": [_stats_source("web", "seg0/stats/0.json")],
            }
        ],
    }
    data = _zip_with(manifest, {"seg0/stats/0.json": json.dumps(payload).encode()})

    sources = read_archive(data)

    assert len(sources) == 1
    assert sources[0].kind == "stats"
    assert sources[0].swarm_services == frozenset({"web"})
    assert sources[0].stats_series["web"] == [(1000.0, 12.5, 30.0, 1024, 500.0)]


def test_read_archive_legacy_no_segments_key() -> None:
    """Pre-v3 manifests have no "segments" key at all -- from/to/created/
    sources live at the top level instead. `_read_segments`'s own
    docstring in server.py calls this the "legacy single-segment shape".
    """
    log_rows = [{"ts": 1000.0, "text": "legacy"}]
    manifest = {
        "from": 1000.0,
        "to": 1000.0,
        "created": "2026-08-14T12:00:00Z",
        "sources": [_log_source("web", "logs/0.jsonl", log_rows)],
    }
    data = _zip_with(manifest, {"logs/0.jsonl": json.dumps(log_rows[0]).encode()})

    sources = read_archive(data)

    assert len(sources) == 1
    assert sources[0].log_rows[0].text == "legacy"


def test_read_archive_multi_segment_requires_explicit_choice() -> None:
    manifest = {
        "version": 3,
        "segments": [
            {
                "from": 1000.0,
                "to": 2000.0,
                "created": "2026-08-14T12:00:00Z",
                "sources": [_log_source("web", "seg0/logs/0.jsonl", [{"ts": 1000.0, "text": "a"}])],
            },
            {
                "from": 3000.0,
                "to": 4000.0,
                "created": "2026-08-14T12:05:00Z",
                "sources": [_log_source("web", "seg1/logs/0.jsonl", [{"ts": 3000.0, "text": "b"}])],
            },
        ],
    }
    data = _zip_with(
        manifest,
        {
            "seg0/logs/0.jsonl": json.dumps({"ts": 1000.0, "text": "a"}).encode(),
            "seg1/logs/0.jsonl": json.dumps({"ts": 3000.0, "text": "b"}).encode(),
        },
    )

    with pytest.raises(MultiSegmentArchive) as exc_info:
        read_archive(data)
    assert len(exc_info.value.segments) == 2

    sources = read_archive(data, segment=1)
    assert sources[0].log_rows[0].text == "b"
