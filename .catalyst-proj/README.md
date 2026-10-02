# Catalyst deployment for log-sump

This project-specific deployment keeps the framework's governance and work artifacts alongside the application code without changing the runtime layout of the service itself.

## Structure

- `rules/` — project rules and rule indexes
- `requirements/` — concrete requirements that drive behavior and tests
- `features/` — descriptive feature entries not tied directly to rules
- `domains/` — domain definitions for the system
- `development/` — bugs, housekeeping, and meta-tag work
- `work-items/` — agile planning artifacts
- `BACKLOG.md` — backlog pointer for the project
- `version.txt` — deployed catalyst version

## Project context

This deployment is for the `log-sump` repository, which collects Docker container logs and metrics, forwards them into Redis Streams, and exposes an authenticated query API.

## Operational note

The deployment directory is intentionally separate from the application source tree so the framework remains discoverable and can be synchronized without mutating the service code.
