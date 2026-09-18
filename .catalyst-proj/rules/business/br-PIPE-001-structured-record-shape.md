# BR-PIPE-001 - Structured records must preserve payload and ordering

## Summary

Every container log line and every sampled metric must be converted into a validated `Record` with a stable `kind`, daemon identity, timestamp, and a monotonic sequence number before it is shipped onward.

## Scope

This rule governs the `log-listener` and `log-server` ingest path, including record creation, field normalization, and Redis stream writing.

## Evidence in the code

- `log-listener` creates `LogRecord` and `MetricRecord` objects from Docker output and metrics snapshots.
- The shared `Record` schema requires `kind`, `docker_host`, `container_name`, `container_id`, `ts`, and `seq` as the common identity fields.
- Ingestion into Redis Streams occurs after validation in the server-side consumer; malformed entries are dropped.

## Acceptance criteria

- Log and metric events are represented as a single schema with a discriminated `kind` field.
- A record always contains the daemon identifier and a container identifier or the `__system__` sentinel for daemon-level metrics.
- The event timestamp and per-source sequence are preserved so ordering remains deterministic inside a daemon stream.
- Records are rejected or dropped if they fail schema validation before being written into Redis.
