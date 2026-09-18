# REQ-001 - Logs and metrics must preserve structure and retention semantics

## Description

The system must convert Docker output and metric snapshots into a validated record format that preserves daemon identity, timing, and ordering while keeping log and metric retention aligned with their distinct sources and purposes.

## Scope

- log-listener
- log-server ingest path
- Redis stream retention policy

## Related rules

- BR-PIPE-001
- BR-PIPE-002

## Acceptance criteria

- Log and metric events are stored with the same required identity and sequencing metadata.
- Container and daemon metrics remain separated and queryable by kind and source.
- Log and metric retention are independently enforced and do not leak across each other.
