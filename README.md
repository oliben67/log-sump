# log-sump

An async service that watches one or more Docker daemons over SSH, streams
each container's logs and resource metrics into a normalized, globally
unique record, ships them through Logstash into Redis Streams with
configurable per-kind retention, and exposes an authenticated query API to
clients.

Written as strictly asynchronous Python (asyncio) end to end — both the
ingestion side (`log-listener`) and the query side (`log-server`).

## Documentation

- [Architecture](docs/architecture.md) — components, data flow, record
  schema, Redis data model, packaging
- [API](docs/api.md) — `log-server`'s REST endpoints, auth, request/response
  shapes
- [Technologies](docs/technologies.md) — the stack, and why each piece was
  chosen

## Quickstart

Prerequisites: [Docker](https://www.docker.com/), [uv](https://docs.astral.sh/uv/),
[`task`](https://taskfile.dev) (optional but recommended — see
[`Taskfile.yml`](Taskfile.yml) for the plain `uv run ...` equivalents).

```bash
task sync              # install log-sump + dev deps
cp config/config.example.yaml config/config.yaml
$EDITOR config/config.yaml   # add the daemon(s) you want to watch
```

**Local dev loop** (no Docker image build required): `task dev:up` starts a
throwaway Redis plus a couple of fixture containers; `log-listener` and
`log-server` then run directly on the host against them.

```bash
task dev:up
task run:listener      # LOG_SUMP_CONFIG_FILE=dev/config.dev.yaml by default
task run:server        # in another shell
```

**Full container image** (all four processes under supervision — see
[Architecture](docs/architecture.md)):

```bash
task docker:build
task compose:up        # reads ./config/config.yaml, serves the API on :8080
```

**Tests:**

```bash
task test              # unit tests, no Docker required (fakeredis)
task dev:up && task test:integration   # exercises a real Redis
task check             # lint + typecheck + test
```

## Project layout

```
src/log_sump/
  common/               # shared: config, Record schema, Transport, Redis keys, auth
  listener/             # discovers containers, streams logs + metrics
  server/               # query API, catalog, Redis Streams ingestion + retention
tests/
  common/ listener/ server/   # unit tests, mirroring src/log_sump/
  integration/          # tests that exercise a real Redis
logstash/               # Logstash pipeline config
supervisor/s6-rc.d/     # s6-overlay service definitions (the 4 supervised processes)
docker/Dockerfile       # multi-stage build for the single-container image
config/                 # config.example.yaml template (config.yaml is gitignored)
```
