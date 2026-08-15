"""Parses the human-readable units `docker stats`/`docker system df` emit
(spec §6: "docker stats emits human-readable strings... Parse percentages to
floats and IEC/SI sizes to integer bytes; keep the original string in
`raw`"). Docker uses IEC (binary, 1024-based) suffixes for memory and SI
(decimal, 1000-based) suffixes for network/block IO/disk usage — both are
handled by the same table here since the suffixes themselves don't overlap
(`MiB` vs `MB`).
"""

from __future__ import annotations

import re

_SIZE_UNITS = {
    "B": 1,
    "KB": 1000,
    "KIB": 1024,
    "MB": 1000**2,
    "MIB": 1024**2,
    "GB": 1000**3,
    "GIB": 1024**3,
    "TB": 1000**4,
    "TIB": 1024**4,
    "PB": 1000**5,
    "PIB": 1024**5,
}

_SIZE_RE = re.compile(r"^([\d.]+)\s*([A-Za-z]+)$")


def parse_bytes(text: str) -> int | None:
    """`"50MiB"` -> `52428800`, `"1.2kB"` -> `1200`. `None` if unparseable."""
    match = _SIZE_RE.match(text.strip())
    if not match:
        return None
    value_str, unit = match.groups()
    multiplier = _SIZE_UNITS.get(unit.upper())
    if multiplier is None:
        return None
    return round(float(value_str) * multiplier)


def parse_pair_bytes(text: str) -> tuple[int | None, int | None]:
    """`"50MiB / 2GiB"` -> `(52428800, 2147483648)`."""
    left, sep, right = text.partition("/")
    if not sep:
        return None, None
    return parse_bytes(left), parse_bytes(right)


def parse_percent(text: str) -> float | None:
    """`"12.34%"` -> `12.34`. `None` if unparseable."""
    text = text.strip()
    if not text.endswith("%"):
        return None
    try:
        return float(text[:-1])
    except ValueError:
        return None
