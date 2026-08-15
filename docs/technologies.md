# Technologies

What log-sump is built with, and why each piece was chosen.

## Language & runtime

**Python 3.12, strictly `asyncio`.** Both `log-listener` and `log-server`
run a single event loop each with no blocking calls on the hot paths —
subprocess I/O, Redis, and HTTP are all async-native. The one deliberate
exception is `python-logstash-async`'s background-thread buffering (see
below); dispatching *into* it is still non-blocking.

## Packaging

- **[uv](https://docs.astral.sh/uv/)** — one package (`pyproject.toml` +
  `uv.lock`) with three subpackages, `log_sump.common` / `.listener` /
  `.server`. Not split into separately-installable distributions: the
  single-image build already installs everything into one shared venv for
  all four supervised processes (`docker/Dockerfile`), so there's no
  runtime dependency-isolation to gain from separate distributions —
  `log_sump.listener` and `log_sump.server` just import what they need
  from `log_sump.common` (config, `Record` schema, `Transport`, auth)
  directly, as ordinary same-package imports.
- **hatchling** — build backend for the `log_sump` package.

## Web framework

**FastAPI + uvicorn.** Pydantic-native request/response models map
directly onto the shared `Record` schema; the dependency-injection system
(`Depends`) is a natural home for the two auth tiers
(`get_permitted_daemons`, `require_valid_api_key`); free OpenAPI docs.
uvicorn also installs `SIGTERM`/`SIGINT` handlers for a graceful ASGI
shutdown without extra code (see `docs/architecture.md`'s packaging
section — `log-listener` has to do this manually, since it isn't an ASGI
app).

## Data validation

**Pydantic v2** (`BaseModel`, `TypeAdapter`) for the `Record` schema
(`LogRecord`/`MetricRecord`, a `kind`-discriminated union) and
**pydantic-settings** for config — one modeling system for both, so config
and schema share the same validation/serialization machinery. Config layers
a YAML file (the daemon catalog, tunables) under environment-variable
overrides (secrets), via a custom `settings_customise_sources`.

## Redis client

**`redis` (redis-py) via `redis.asyncio`** — the current standard async
Redis client (the older `aioredis` project merged into it), with native
`XADD`/`XRANGE`/`XTRIM`/pipelining support.

## Structured logging & shipping

- **structlog** — every captured log line and sampled metric is emitted via
  `await logger.ainfo(...)`. Both the operational/diagnostic logger
  (console, for local dev) and the dedicated records-shipping logger use
  `structlog.stdlib.BoundLogger`'s built-in async methods, which run the
  underlying sync call in a thread executor rather than blocking the loop.
- **python-logstash-async** — ships each record's JSON to Logstash's TCP
  input via a background worker thread with a persistent, on-disk SQLite
  buffer (`database_path`), so in-flight records survive a `log-listener`
  restart rather than being lost. This is the one place strict-async is
  relaxed: the buffer itself is a synchronous, thread-owned queue, but
  dispatching into it never blocks the event loop.

## Log/metric transport & parsing

**Logstash** — receives records over TCP (`json_lines` codec, matching
`python-logstash-async`'s wire format), does minimal validation/tagging,
and forwards the original record JSON — not its own enriched event — into
Redis via the `redis` output. Installed in the container from the official
Elastic tarball release (bundles its own JDK; no system Java needed),
rather than the Docker image, to avoid a cross-distro binary-copy risk
against this project's Debian-based runtime image.

## Storage

**Redis Streams** — chosen over a plain list (which can't expire individual
elements) or a key-per-record + sorted-set index. Streams give real
age-based trimming (`XTRIM MINID`), natural time-ordered unique IDs
(`XADD`'s auto-generated `<ms>-<seq>`), and range queries (`XRANGE`) — see
`docs/architecture.md` for the per-daemon/per-kind stream layout and the
exact-vs-approximate trimming decision.

## Remote execution

**SSH, via `asyncio.create_subprocess_exec` wrapping the system `ssh`
binary** — not a Python SSH library. Reuses whatever SSH setup
(`~/.ssh/config`, agent, known_hosts) is already in place for the operator
running the container, and needs no extra dependency. A `LocalTransport`
implementing the same interface (no `ssh` wrapper) lets local dev/tests run
the identical listener code against a developer's own Docker socket.

## Testing

- **pytest** + **pytest-asyncio** (auto mode) — the test runner.
- **fakeredis** (`FakeAsyncRedis`) — fast, in-memory Redis for most tests;
  no Docker required for the default `task test` run.
- **A real Redis** (via `docker-compose.dev.yml`) for a separate
  `integration`-marked test tier (`tests/integration/`) — needed because
  `fakeredis` missed at least one real behavioral gap (see
  `docs/architecture.md`'s retention note) that only showed up against
  genuine Redis.
- **httpx** (`AsyncClient` + `ASGITransport`) — exercises `log-server`'s
  FastAPI app in-process over real HTTP semantics, without a running
  server process.
- **`--import-mode=importlib`** (pytest) — `tests/{common,listener,server}/`
  repeat test-file basenames across directories (e.g. both `listener/` and
  `server/` have their own `test_app.py`); the default import mode requires
  globally-unique top-level module names and collides on those, so this
  project uses `importlib` mode instead, which identifies modules by full
  path.

## Linting, formatting, type checking

- **[ruff](https://docs.astral.sh/ruff/)** — lint + format, one config at
  the project root.
- **[ty](https://docs.astral.sh/ty/)** (Astral) — static type checking
  across `src/` and `tests/`.

## Task running

**[go-task](https://taskfile.dev)** (`Taskfile.yml`) — thin wrappers around
`uv run ...` / `docker compose ...` for the common dev commands (`sync`,
`test`, `lint`, `dev:up`, `run:listener`, `docker:build`, ...); see
`task --list`.

## Containerization & process supervision

- **Docker**, multi-stage build (`docker/Dockerfile`): a `uv`-based builder
  stage producing the venv, copied into a runtime stage that also carries
  `redis-server`, `openssh-client`, and Logstash.
- **[s6-overlay](https://github.com/just-containers/s6-overlay)** v3 — the
  four processes are supervised with explicit start-order dependencies
  (`redis ← logstash ← log-listener`, `redis ← log-server`) rather than a
  single foreground process, per the project's explicit single-container,
  four-process requirement. Chosen over `supervisord`: proper PID-1 zombie
  reaping (relevant since `ssh`/`docker` child processes can leave orphans)
  and native dependency ordering.
