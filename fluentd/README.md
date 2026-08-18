# Migration note: Logstash → Fluentd

Replaces the `logstash` service (previously `logstash/`, the official
Elastic tarball) with `fluentd` (`gem install fluentd`), behavior-preserving
per the migration prompt this implements. `log-listener` and `log-server`
were not touched — everything below is confined to the transport/parse
stage between them.

## Open questions, answered from the actual code (not inferred)

1. **Input protocol/codec**: plain TCP, newline-delimited JSON. Confirmed
   directly from the installed `logstash_async` package
   (`transport.py`'s `TcpTransport._send_via_socket` /
   `handler.py`'s `_format_record` returning `formatted + b'\n'`), not just
   the old `.conf`'s `codec => json_lines` — `AsynchronousLogstashHandler`
   defaults to `TcpTransport`, `ssl_enable=False`.
2. **Exact Redis output**: `RPUSH logsump:ingest:v1 <raw message string>`
   — a plain list, **not** a stream. Logstash's `redis` output used
   `data_type => "list"` with `codec => plain { format => "%{message}" }`
   (the untouched original record string, not a re-serialized event).
   Confirmed both from the `.conf` and from `log_sump.server.ingest.
   consumer`'s own docstring ("Logstash's redis output can only RPUSH to a
   list -- it has no native XADD output"). Pipelined (Logstash batches
   writes), but semantically one RPUSH per record either way.
3. **Interleaving guarantee**: on **write**, not read. `container_listener.
   py`, `container_stats.py`, `system_stats.py`, and `services_listing.py`
   all log through the *same* `records_logger` → the *same*
   `AsynchronousLogstashHandler` → one TCP connection → Logstash's single
   `pipeline.workers: 1` → sequential RPUSH. `log-server`'s consumer then
   drains FIFO (`BLPOP` + batched `LPOP`). This is why the new output
   plugin's `<buffer>` pins `flush_thread_count 1`.
4. **TTL**: none, at this stage. The ingest list is a transient queue
   (drained continuously); retention (`XTRIM`) is applied later, by
   `log-server`, to the Streams — unaffected by this change.
5. **Driver for the swap**: not independently established here — this
   implements a prompt that specified Fluentd directly. Worth noting for
   context: a live diagnosis of an unrelated SSH-tunnel bug against this
   project's own remote deployment (2026-08-17) found the Logstash
   container's *actual* memory/CPU footprint was not a real problem on that
   host (1.67GB/31GB, 0.42% CPU) -- so if footprint specifically motivated
   this, it wasn't runtime memory pressure; more plausibly installer/image
   size (the bundled Windows installer and the image tarball both carry
   whatever's in this container).

## Fluentd vs. Fluent Bit

Not reconsidered here, for a concrete technical reason beyond "the prompt
said Fluentd": achieving exact Redis parity (RPUSH the untouched original
string to a specific list key) needs either a maintained plugin that does
that, or a small custom plugin. Neither existing Fluentd Redis plugin
checked reproduces it —
[`fluent-plugin-redis`](https://github.com/fluent-plugins-nursery/fluent-plugin-redis)
(the most actively maintained one, under Fluentd's own plugin nursery org)
is oriented at `SET`-style key writes (`insert_key_prefix`, `ttl`, no
`data_type`/list option); [`fluent-plugin-redislist`](https://github.com/kensantou/fluent-plugin-redislist)
does target lists but looks effectively unmaintained (4 commits total, no
visible key-configurability) and wasn't trusted for production use. That
leaves a custom plugin either way -- and Fluentd's mature Ruby plugin API
(`Fluent::Plugin::Output`, real `<buffer>`/retry integration, the official
`redis` gem) makes that a ~100-line file. Fluent Bit's plugin story for
this (C core, Lua/WASM scripting, no direct equivalent to dropping in a
`redis` gem call) would make the *same* custom-sink requirement
meaningfully harder, not easier -- so Fluent Bit wasn't the better fit
regardless of what's driving the swap.

## Chosen approach: a small custom output plugin

`plugin/out_log_sump_redis_list.rb` (`Fluent::Plugin::Output`, buffered,
`flush_thread_count 1`): parses each record's `message` field as JSON
purely to validate (`kind`/`docker_host` present — matching the old
`.conf`'s `json` filter + `tag_on_failure` + drop-on-invalid), then `RPUSH`s
the **original, unparsed string** — never a re-serialized copy — to the
configured key via the `redis` gem, pipelined per chunk. Buffer failures
raise, so Fluentd's own retry/backoff (`retry_forever true`) handles a
Redis outage; this doesn't weaken the durability floor, which was always
`python-logstash-async`'s own on-disk buffer on the `log-listener` side,
unchanged by this migration either way.

## Verification (live, not just config review)

Built a throwaway image (Debian + `gem install fluentd`/`redis`, same
approach as the real Dockerfile) and ran it against a real Redis container,
driven by the *actual* `logstash_async.handler.AsynchronousLogstashHandler`
(same class `log-listener` uses) sending real `LogRecord`/`MetricRecord`
JSON:

- **Parity**: sent one valid log record and one valid metric record.
  `LRANGE logsump:ingest:v1 0 -1` returned exactly those two entries,
  byte-identical to the JSON sent, in send order.
- **Drop behavior**: sent one malformed-JSON message and one
  schema-invalid-but-valid-JSON message (`{"foo": "bar"}`). Neither reached
  Redis; the plugin logged
  `dropping unparsable record error="unexpected token at '{not valid json'"`
  and `dropping schema-invalid record (missing kind/docker_host)`
  respectively — matching the old pipeline's tag-and-drop behavior.
- **Failure semantics**: stopped the Redis container, sent another valid
  record (buffered, not lost — `write` raised, Fluentd retried with
  backoff), restarted Redis, confirmed `retry succeeded` in the log and the
  record present in the list afterward, in its correct arrival-order
  position (after the two earlier records) — no data loss, no reordering,
  across a real outage.

## Removed

- `logstash/` (config directory), `LOGSTASH_VERSION` build arg, the
  Logstash tarball-fetch `RUN` block, in both `docker/Dockerfile` (log-sump)
  and `app/log-sump-extended/Dockerfile` (which fetched the same config
  from this repo at build time).
- `supervisor/s6-rc.d/logstash/` → renamed to `.../fluentd/` (same
  redis-wait `run` script logic, execs `fluentd` instead);
  `log-listener`'s `dependencies.d/logstash` → `dependencies.d/fluentd`.
