# BUG-002 - Unauthorized daemon access must be blocked

## Description

The API must reject requesters that do not have the correct key or the correct daemon permission. Any path that exposes daemon data without the correct scope is a security and contract violation.

## Root cause

Authorization is enforced at the API boundary through API key validation and daemon-scoped permission checks. Missing or incomplete checks can allow keys to access unrelated daemons or read-only Redis functions without appropriate permissions.

## Targets

- UR-API-001

## Reproduction

1. Present a valid API key without permission for a requested daemon.
2. Observe the response from `/catalog` or `/records`.
3. Attempt a Redis read on an unauthorized daemon and verify the access is blocked.
4. Verify that valid-but-scoped requests still succeed only for permitted daemons.
