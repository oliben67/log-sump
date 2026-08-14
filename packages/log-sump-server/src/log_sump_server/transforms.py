"""User-supplied per-record transform plugins (migration plan Phase 5).
Direct translation of cttc's own `TransformRegistry`/`apply_transforms`
(`server.py`) -- log-only, matching cttc exactly: `StatsSource` never
routed records through this, only `LogSource` did.

Applied in `ingest/consumer.py`'s `_ingest_batch`, immediately before the
existing validate-then-`XADD` step, to every incoming `LogRecord` (the
Logstash-fed live path only -- not the local-upload path, a deliberate
scope boundary for this pass: an uploaded/imported archive is typically
either an already-processed cttc export or plain text where a transform
matters far less than for noisy live collection).
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from pathlib import Path

import structlog
from log_sump_common.schema import LogRecord, RecordAdapter
from pydantic import ValidationError

logger = structlog.get_logger(__name__)

#: A transform module's `transform(record)` function: a record dict in,
#: record dict(s) or `None` (drop) out.
TransformFn = Callable[[dict], "dict | list[dict] | None"]


class TransformRegistry:
    """User modules in `directory`. Each exposes `transform(record) ->
    dict | list[dict] | None` (`None` drops the record).
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def available(self) -> list[dict]:
        out = []
        if not self.directory.is_dir():
            return out
        for p in sorted(self.directory.glob("*.py")):
            if p.name.startswith("_"):
                continue
            doc = ""
            try:
                for line in p.read_text(errors="replace").splitlines():
                    line = line.strip()
                    if line.startswith(('"""', "'''", "#")):
                        doc = line.strip("\"'# ")
                        break
                    if line:
                        break
            except OSError as e:
                logger.debug("transforms.doc_read_failed", path=str(p), error=str(e))
            out.append({"name": p.stem, "doc": doc})
        return out

    def load(self, names: list[str]) -> list[tuple[str, TransformFn]]:
        fns: list[tuple[str, TransformFn]] = []
        for name in names:
            path = self.directory / f"{name}.py"
            if not path.is_file():
                raise ValueError(f"transform not found: {name}")
            spec = importlib.util.spec_from_file_location(f"logsump_transform_{name}", path)
            if spec is None or spec.loader is None:
                raise ValueError(f"transform not found: {name}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            fn = getattr(mod, "transform", None)
            if not callable(fn):
                raise ValueError(f"transform module {name} has no transform() function")
            fns.append((name, fn))
        return fns


def apply_transforms(
    record: LogRecord, transform_fns: list[tuple[str, TransformFn]]
) -> list[LogRecord]:
    """cttc's own `apply_transforms`, ported: a record dict in, record
    dict(s) or `None` out per transform, chained. Operates on
    `record.model_dump()` at the boundary and re-validates each surviving
    dict back into a `LogRecord` -- output that no longer validates is
    dropped (logged), not allowed to corrupt a stream with a malformed
    entry, matching `ingest/consumer.py`'s own established bias.
    """
    records: list[dict] = [record.model_dump(mode="json")]
    for name, fn in transform_fns:
        nxt: list[dict] = []
        for r in records:
            try:
                out = fn(r)
            except Exception as e:  # a broken user module must not kill ingest
                r.setdefault("fields", {})["_transform_error"] = f"{name}: {e}"
                nxt.append(r)
                continue
            if out is None:
                continue
            nxt.extend(out if isinstance(out, list) else [out])
        records = nxt

    result: list[LogRecord] = []
    for r in records:
        try:
            validated = RecordAdapter.validate_python(r)
        except ValidationError as e:
            logger.warning("transforms.output_invalid", error=str(e))
            continue
        if isinstance(validated, LogRecord):
            result.append(validated)
    return result
