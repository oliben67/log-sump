# LOG-PIPE - Record pipeline

## Scope

The ingestion and retention path for log and metric records from remote Docker daemons through Logstash into Redis Streams and back out through the query API.

## Responsibilities

- Convert Docker and system events into a single typed record schema.
- Preserve ordering and daemon provenance while routing events by kind.
- Keep log and metrics retention separate and bounded to their own time horizons.
- Ensure malformed records are rejected before they reach the persisted stream layer.
