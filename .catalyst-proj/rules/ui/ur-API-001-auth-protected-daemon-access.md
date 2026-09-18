# UR-API-001 - Daemon access is authenticated and scoped

## Summary

Every request that reads daemon data or Redis inspection data must be authenticated with a valid API key, and daemon-scoped reads must be limited to the set of daemons explicitly permitted for that key.

## Scope

This rule covers the HTTP API and its auth backend in `log-sump-server`.

## Evidence in the code

- API requests require an `X-API-Key` header.
- `RedisApiKeyAuthBackend.permitted_daemons()` resolves the set of permitted daemon ids for a key.
- `/catalog` and `/records` are daemon-scoped, while `/admin/redis/command` is any-valid-key scoped.

## Acceptance criteria

- Requests without a valid API key receive `401 Unauthorized`.
- Requests with a valid key but an unauthorized daemon receive `403 Forbidden`.
- `/catalog` and `/records` never expose data for a daemon outside the caller's permission set.
- Redis inspection remains read-only and restricted to an allowlisted command list.
