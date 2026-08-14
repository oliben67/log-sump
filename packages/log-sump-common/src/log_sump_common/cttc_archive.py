"""Reader for cttc's `.cttc-metric`/`.cttc-record` archive format (zip +
`manifest.json`) -- migration plan Phase 2. Unchanged from cttc's own
`cttc_format.py`/`server.py` (`_write_segment`/`_read_segments`/
`build_sample_bytes`/`load_sample`) so a sample exported by today's
`app/server` reads back byte-for-byte compatible here.

Dependency-free (no Redis/FastAPI ties), like cttc's own `cttc_format.py`
was kept, so both `log-sump-server`'s upload router and any future export
path can use it without a circular import.

Only the *reader* lives here for now: Phase 2 covers importing a sample;
producing one from log-sump's own Streams is scheduled alongside recording
sessions (Phase 4), which is the feature that actually needs to write one.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass, field

METRIC_EXT = ".cttc-metric"
RECORD_EXT = ".cttc-record"


def is_cttc_archive(name: str) -> bool:
    return name.endswith(METRIC_EXT) or name.endswith(RECORD_EXT)


class MultiSegmentArchive(ValueError):
    """Raised by `read_archive` when the .cttc holds more than one recorded
    segment and no `segment` index was given -- mirrors cttc's own
    `MultiSegmentSample`, so a caller can ask which one to load instead of
    one being picked silently. `segments` carries each one's own
    from/to/created/source_count, same shape cttc's exception does.
    """

    def __init__(self, segments: list[dict]):
        super().__init__(f"archive holds {len(segments)} segments -- pick one via `segment=`")
        self.segments = segments


@dataclass(frozen=True)
class ArchivedLogRow:
    ts_ms: float
    text: str


#: One (ts_ms, cpu_pct, mem_pct, mem_bytes, net_rate_bps) stats row.
#: `net_rate_bps` is cttc's own already-computed rate (bytes/sec), not a
#: cumulative counter -- see the migration plan's Phase 2 notes on why
#: log-sump's importer can't project this back into net_rx_bytes/
#: net_tx_bytes without fabricating values.
StatsRow = tuple[float, float | None, float | None, float | None, float | None]


@dataclass(frozen=True)
class ArchivedSource:
    kind: str  # "log" | "stats"
    name: str
    log_rows: list[ArchivedLogRow] = field(default_factory=list)
    stats_series: dict[str, list[StatsRow]] = field(default_factory=dict)
    #: Service names among this segment's stats sources that cttc had
    #: already identified as swarm-merged (see cttc's `StatsSource._swarm`)
    #: -- used to reconstruct a dotted `<service>.imported.<n>` container
    #: name on import, so log-sump's own `_metric_group` re-detects it as a
    #: service instead of silently downgrading it to "container".
    swarm_services: frozenset[str] = frozenset()


def _manifest_hash(manifest_without_hash: dict) -> str:
    """sha256 over the canonical (sorted-keys) JSON of a manifest, matching
    cttc's own `_manifest_hash` -- must produce the identical digest for
    the integrity check below to mean anything.
    """
    payload = json.dumps(manifest_without_hash, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def read_archive(data: bytes, *, segment: int | None = None) -> list[ArchivedSource]:
    """One segment's sources from a `.cttc-metric`/`.cttc-record` archive.

    Raises `MultiSegmentArchive` if the archive holds more than one segment
    and `segment` wasn't given. A manifest integrity-hash mismatch is
    tamper-evidence only (matches cttc's own permissive-read bias) --
    logged by the caller if it cares, not raised here.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        manifest = json.loads(z.read("manifest.json"))
        manifest.pop("integrity_sha256", None)
        raw_segments = manifest.get("segments")
        if raw_segments is None:
            raw_segments = [
                {
                    "from": manifest.get("from"),
                    "to": manifest.get("to"),
                    "created": manifest.get("created"),
                    "sources": manifest.get("sources", []),
                }
            ]
        if len(raw_segments) > 1 and segment is None:
            raise MultiSegmentArchive(
                [
                    {
                        "index": i,
                        "from": seg["from"],
                        "to": seg["to"],
                        "created": seg.get("created"),
                        "source_count": len(seg["sources"]),
                    }
                    for i, seg in enumerate(raw_segments)
                ]
            )
        seg = raw_segments[segment or 0]

        sources: list[ArchivedSource] = []
        for meta in seg["sources"]:
            content = z.read(meta["file"])
            if meta["type"] == "log":
                rows = []
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    rows.append(ArchivedLogRow(ts_ms=row["ts"], text=row.get("text", "")))
                sources.append(ArchivedSource(kind="log", name=meta["name"], log_rows=rows))
            else:
                payload = json.loads(content)
                series = {
                    svc: [tuple(row) for row in rows]
                    for svc, rows in payload.get("series", {}).items()
                }
                sources.append(
                    ArchivedSource(
                        kind="stats",
                        name=meta["name"],
                        stats_series=series,
                        swarm_services=frozenset(payload.get("swarm", [])),
                    )
                )
        return sources
