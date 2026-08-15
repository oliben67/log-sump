"""Shared record schema for log-sump.

Every captured log line and every sampled metric is normalized into one of the
two `Record` variants below, discriminated by `kind`. Both variants share the
same identity fields (`docker_host`, `container_name`, `container_id`, `ts`,
`seq`) so they land contiguously on one per-daemon timeline. `seq` is the
uniqueness tie-breaker: `docker_host`/`container_name`/`container_id`/`ts`
alone can collide when two lines share a sub-second timestamp.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

SYSTEM_SCOPE_ID = "__system__"


class Kind(StrEnum):
    LOG = "log"
    METRIC = "metric"
    SERVICE = "service"


class RecordBase(BaseModel):
    docker_host: str
    container_name: str
    container_id: str
    ts: datetime
    seq: int


class LogRecord(RecordBase):
    kind: Literal[Kind.LOG] = Kind.LOG
    stream: Literal["stdout", "stderr"]
    level: str
    message: str
    fields: dict[str, Any] = Field(default_factory=dict)
    raw: str


class MetricRecord(RecordBase):
    kind: Literal[Kind.METRIC] = Kind.METRIC
    metric_scope: Literal["container", "system"]
    cpu_pct: float | None = None
    mem_used_bytes: int | None = None
    mem_limit_bytes: int | None = None
    mem_pct: float | None = None
    net_rx_bytes: int | None = None
    net_tx_bytes: int | None = None
    blk_read_bytes: int | None = None
    blk_write_bytes: int | None = None
    pids: int | None = None
    system: dict[str, Any] | None = None
    source: str
    raw: Any = None


class ServiceRecord(BaseModel):
    """One swarm service, from one `docker service ls` listing cycle -- for
    a client to offer a whole service, not one task/container, as a
    collection target. Not a `RecordBase`: a service isn't a container, so
    it carries no `container_name`/`container_id`. Every service from the same listing
    cycle shares that cycle's own `ts`, which `queries.latest_services`
    relies on to find "the current listing" without a separate snapshot key.
    """

    kind: Literal[Kind.SERVICE] = Kind.SERVICE
    docker_host: str
    ts: datetime
    seq: int
    id: str
    name: str
    replicas: str


Record = Annotated[LogRecord | MetricRecord | ServiceRecord, Field(discriminator="kind")]


class RecordAdapter:
    """Validates/serializes the `Record` discriminated union.

    Pydantic's `TypeAdapter` doesn't get generated automatically for a bare
    `Annotated[Union[...]]` alias, so this module owns a single shared adapter
    rather than each caller constructing its own.
    """

    _adapter: TypeAdapter[LogRecord | MetricRecord | ServiceRecord] = TypeAdapter(Record)

    @classmethod
    def validate_json(cls, data: str | bytes) -> LogRecord | MetricRecord | ServiceRecord:
        return cls._adapter.validate_json(data)

    @classmethod
    def validate_python(cls, data: Any) -> LogRecord | MetricRecord | ServiceRecord:
        return cls._adapter.validate_python(data)

    @classmethod
    def dump_json(cls, record: LogRecord | MetricRecord | ServiceRecord) -> bytes:
        return cls._adapter.dump_json(record)
