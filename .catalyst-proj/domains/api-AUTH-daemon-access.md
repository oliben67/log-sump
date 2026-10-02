# API-AUTH - Daemon authorization

## Scope

The API and authorization layer that governs access to daemon data, server-side Redis inspection, and the request/response security model for clients.

## Responsibilities

- Authenticate requests with a valid API key.
- Enforce daemon-scoped authorization for `catalog` and `records` reads.
- Restrict Redis inspection to allowlisted commands and read-only semantics.
- Return consistent `401` and `403` responses when access or authorization fails.
