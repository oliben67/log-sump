"""Migration plan Phase 5: TransformRegistry/apply_transforms -- direct
translation of a prior gateway implementation's own
TransformRegistry/apply_transforms (server.py).
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from log_sump.common.schema import LogRecord
from log_sump.server.transforms import TransformRegistry, apply_transforms

DROP_HEALTHCHECKS = '''"""Drops any log line mentioning a healthcheck."""

def transform(record):
    if "healthcheck" in record.get("message", "").lower():
        return None
    return record
'''

UPPERCASE_MESSAGE = '''"""Uppercases every message."""

def transform(record):
    record = dict(record)
    record["message"] = record["message"].upper()
    return record
'''

SPLIT_MULTILINE = '''"""Splits a newline-joined message into two records."""

def transform(record):
    if "\\n" not in record.get("message", ""):
        return record
    lines = record["message"].split("\\n")
    out = []
    for line in lines:
        r = dict(record)
        r["message"] = line
        out.append(r)
    return out
'''

BROKEN = '''"""Always raises."""

def transform(record):
    raise RuntimeError("boom")
'''

NOT_A_TRANSFORM = '''"""No transform() function at all."""

def not_transform(record):
    return record
'''


def _record(message: str = "hello") -> LogRecord:
    return LogRecord(
        docker_host="daemon-a",
        container_name="web",
        container_id="c1",
        ts=datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC),
        seq=1,
        stream="stdout",
        level="info",
        message=message,
        fields={},
        raw=message,
    )


@pytest.fixture
def transforms_dir(tmp_path: Path) -> Path:
    (tmp_path / "drop_healthchecks.py").write_text(DROP_HEALTHCHECKS)
    (tmp_path / "uppercase_message.py").write_text(UPPERCASE_MESSAGE)
    (tmp_path / "split_multiline.py").write_text(SPLIT_MULTILINE)
    (tmp_path / "broken.py").write_text(BROKEN)
    (tmp_path / "not_a_transform.py").write_text(NOT_A_TRANSFORM)
    (tmp_path / "_private.py").write_text(UPPERCASE_MESSAGE)  # leading underscore -- hidden
    return tmp_path


def test_available_lists_modules_with_their_docstring(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    available = {t["name"]: t["doc"] for t in registry.available()}
    assert available["drop_healthchecks"] == "Drops any log line mentioning a healthcheck."
    assert "_private" not in available  # leading underscore is hidden


def test_available_on_missing_directory_returns_empty(tmp_path: Path) -> None:
    registry = TransformRegistry(tmp_path / "does-not-exist")
    assert registry.available() == []


def test_load_unknown_transform_raises(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    with pytest.raises(ValueError, match="transform not found"):
        registry.load(["nonexistent"])


def test_load_module_without_transform_function_raises(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    with pytest.raises(ValueError, match="has no transform"):
        registry.load(["not_a_transform"])


def test_apply_transforms_drops_matching_record(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["drop_healthchecks"])
    out = apply_transforms(_record("GET /healthcheck 200"), fns)
    assert out == []


def test_apply_transforms_passes_through_non_matching_record(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["drop_healthchecks"])
    out = apply_transforms(_record("normal request"), fns)
    assert len(out) == 1
    assert out[0].message == "normal request"


def test_apply_transforms_can_edit_the_record(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["uppercase_message"])
    out = apply_transforms(_record("hello"), fns)
    assert out[0].message == "HELLO"


def test_apply_transforms_can_expand_one_record_into_many(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["split_multiline"])
    out = apply_transforms(_record("line one\nline two"), fns)
    assert [r.message for r in out] == ["line one", "line two"]


def test_apply_transforms_chains_multiple_in_order(transforms_dir: Path) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["drop_healthchecks", "uppercase_message"])
    out = apply_transforms(_record("normal request"), fns)
    assert out[0].message == "NORMAL REQUEST"


def test_apply_transforms_broken_module_annotates_and_keeps_the_record(
    transforms_dir: Path,
) -> None:
    registry = TransformRegistry(transforms_dir)
    fns = registry.load(["broken"])
    out = apply_transforms(_record("hello"), fns)
    assert len(out) == 1
    assert "_transform_error" in out[0].fields
    assert "boom" in out[0].fields["_transform_error"]
