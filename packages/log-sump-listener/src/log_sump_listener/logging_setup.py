"""structlog configuration.

Two structlog loggers exist side by side, deliberately configured
differently:

- The **global** logger (`configure_logging` / plain `structlog.get_logger()`)
  is for operational/diagnostic messages (app lifecycle, daemon
  reachability, tracker decisions) — rendered to the console for local
  dev visibility.
- The **records** logger (`build_records_logger`) is built separately via
  `structlog.wrap_logger` rather than the global config, dedicated to
  shipping already-serialized `Record` JSON (spec §6) to Logstash through
  `python-logstash-async`'s `AsynchronousLogstashHandler`. Keeping it off
  the global config means high-volume captured container log lines never
  flow through (or get mixed into) console rendering.

Both rely on `structlog.stdlib.BoundLogger`'s built-in `a<level>` methods
(`await logger.ainfo(...)`), which run the underlying sync stdlib logging
call in a thread executor — never blocking the event loop. That's what
makes `python-logstash-async`'s own background-thread buffering (spec
§5.2/§11.4) safe to use here: the *dispatch* into that buffer is
non-blocking even though the buffer itself is a synchronous, thread-owned
SQLite-backed queue.

The records logger passes its event straight through as the stdlib log
message with **no extra kwargs** (`processors=[render_to_log_kwargs]`,
called as `logger.ainfo(json_string)`): container_listener.py hands it an
already-`RecordAdapter.dump_json`-serialized string, and nothing here
re-renders or wraps it. `logstash/pipeline/log-sump.conf` reads that string
back out via `%{message}`, so the Redis list entry ends up being exactly
that JSON, unmodified — see the module docstring there for why.
"""

from __future__ import annotations

import logging
import sys
from typing import Protocol

import structlog
from logstash_async.handler import AsynchronousLogstashHandler


class RecordsLogger(Protocol):
    """Structural type for the records logger: just the one method callers use.

    `structlog.stdlib.BoundLogger` (what `build_records_logger` returns)
    satisfies this structurally, and so does a plain test double — callers
    should type against this Protocol rather than the concrete
    `structlog.stdlib.BoundLogger` class so tests don't need a real
    structlog/stdlib logger wired up.
    """

    async def ainfo(self, event: str) -> None: ...


#: Name of the stdlib logger the `AsynchronousLogstashHandler` attaches to.
#: `propagate = False` keeps it off the console handler set up by
#: `configure_logging` — see module docstring.
RECORDS_LOGGER_NAME = "log_sump.records"


def configure_logging(*, level: int = logging.INFO) -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def build_records_logger(
    *, logstash_host: str, logstash_port: int, database_path: str
) -> structlog.stdlib.BoundLogger:
    """Build the dedicated Record-shipping logger described above.

    Safe to call more than once (e.g. across tests): the underlying stdlib
    logger's handlers are cleared first so repeated calls don't stack up
    duplicate `AsynchronousLogstashHandler`s sending each record twice.
    """
    stdlib_logger = logging.getLogger(RECORDS_LOGGER_NAME)
    stdlib_logger.setLevel(logging.INFO)
    stdlib_logger.propagate = False
    stdlib_logger.handlers.clear()
    stdlib_logger.addHandler(
        AsynchronousLogstashHandler(logstash_host, logstash_port, database_path=database_path)
    )
    return structlog.wrap_logger(
        stdlib_logger,
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[structlog.stdlib.render_to_log_kwargs],
    )


def close_records_logger() -> None:
    """Flush and close the records logger's handler (spec §10: "flush the
    logstash buffer" as part of graceful shutdown).

    Best-effort: `database_path` is a persistent on-disk buffer (spec
    §5.2/§11.4), so anything not flushed here survives process exit and
    gets retried on the next start rather than being lost outright.
    """
    stdlib_logger = logging.getLogger(RECORDS_LOGGER_NAME)
    for handler in stdlib_logger.handlers:
        handler.flush()
        handler.close()
