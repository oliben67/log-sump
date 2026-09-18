# REQ-002 - Authorized daemon access must be enforced for API consumers

## Description

The API must enforce daemon-scoped permission checks for daemon data and allow only permitted Redis inspection commands for known valid keys.

## Scope

- log-server API
- auth backend
- Redis inspection routes

## Related rules

- UR-API-001

## Acceptance criteria

- Requests without a valid API key are rejected.
- Valid keys may only access daemons they are explicitly permitted to read.
- Invalid daemon access returns `403 Forbidden`.
- Redis admin inspection is constrained to allowed commands only.
