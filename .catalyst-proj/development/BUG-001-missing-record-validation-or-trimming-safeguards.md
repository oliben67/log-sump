# BUG-001 - Ingest and retention logic must be validated and bounded

## Description

The record pipeline is only reliable if it enforces schema validation and exact retention trimming. Missing validation or approximate trimming can silently let malformed or stale entries remain in the system, undermining the contract around record integrity.

## Root cause

The project’s architecture requires schema enforcement in Python and exact `XTRIM MINID` trimming to avoid stale rows surviving in a Redis stream. A failure here leaves the pipeline with unvalidated records or retention behavior that is not exact.

## Targets

- BR-PIPE-001
- BR-PIPE-002

## Reproduction

1. Feed malformed or partial records into the ingest path.
2. Observe whether validation blocks them before stream insertion.
3. Check stream retention behavior against the configured time horizon.
4. Confirm that expired log or metric entries are compactly trimmed instead of lingering beyond the exact boundary.
