"""container-listener (spec §5.1): one task per `(daemon, container)`.

Streams a container's logs continuously via `docker logs -f --timestamps
<container>` over the transport, splits the RFC3339Nano timestamp
`--timestamps` prepends from the rest of the line, JSON-detects the
remainder — a native JSON payload contributes its own `level`/`message`/
extra fields, anything else becomes a plain message with a default level —
and ships the resulting `LogRecord`, pre-serialized to JSON, through the
dedicated records logger built by `logging_setup.build_records_logger`.
"""

from __future__ import annotations

import itertools
import json
from datetime import datetime

from log_sump_common.schema import LogRecord, RecordAdapter
from log_sump_common.transport import StreamLine, Transport

from .logging_setup import RecordsLogger

DEFAULT_LEVEL = "info"


async def run_container_listener(
    docker_host: str,
    container_id: str,
    container_name: str,
    transport: Transport,
    records_logger: RecordsLogger,
) -> None:
    """Run until cancelled. Cancellation kills the underlying subprocess
    (guaranteed by `Transport.stream_lines`), so callers can stop a listener
    just by cancelling this task.
    """
    seq_counter = itertools.count(1)
    async with transport.stream_lines(
        ["docker", "logs", "-f", "--timestamps", container_id]
    ) as lines:
        async for line in lines:
            record = _parse_line(
                docker_host=docker_host,
                container_id=container_id,
                container_name=container_name,
                line=line,
                seq=next(seq_counter),
            )
            if record is None:
                continue
            await records_logger.ainfo(RecordAdapter.dump_json(record).decode())


def _parse_line(
    *,
    docker_host: str,
    container_id: str,
    container_name: str,
    line: StreamLine,
    seq: int,
) -> LogRecord | None:
    ts_str, _, payload = line.text.partition(" ")
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except ValueError:
        # Not a `--timestamps`-prefixed line — shouldn't normally happen
        # since we always pass that flag, but drop rather than crash.
        return None

    level = DEFAULT_LEVEL
    message = payload
    fields: dict[str, object] = {}
    parsed = _try_parse_json_object(payload)
    if parsed is not None:
        fields = parsed
        message = str(fields.pop("message", payload))
        level = str(fields.pop("level", DEFAULT_LEVEL))

    return LogRecord(
        docker_host=docker_host,
        container_name=container_name,
        container_id=container_id,
        ts=ts,
        seq=seq,
        stream=line.stream,
        level=level,
        message=message,
        fields=fields,
        raw=line.text,
    )


def _try_parse_json_object(payload: str) -> dict[str, object] | None:
    try:
        parsed = json.loads(payload)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None
