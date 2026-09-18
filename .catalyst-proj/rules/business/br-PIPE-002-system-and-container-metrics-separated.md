# BR-PIPE-002 - Metrics are partitioned by source and retention horizon

## Summary

Container metrics and daemon-level system metrics must remain distinct and be retained under a policy that matches their kind and source, so the history remains interpretable and bounded.

## Scope

This rule applies to monitoring, ingestion, and retention behavior in the listener and server components.

## Evidence in the code

- `container_stats.py` emits per-container metrics at a per-daemon cadence.
- `system_stats.py` emits daemon-level metrics with `container_id = "__system__"`.
- Redis streams are keyed by `docker_host` and `kind`, with separate retention settings for logs and metrics.

## Acceptance criteria

- Container metrics and system metrics are tracked separately in the ingest pipeline.
- Daemon-level metrics are clearly tagged and can be queried without conflating them with container metrics.
- Log and metric retention are independently configured and trimmed to their own time windows.
- Querying both record kinds produces a combined timeline without corrupting either stream's data boundaries.
