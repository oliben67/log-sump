"""CLI entrypoint: `python -m log_sump_listener` (spec §5.1 orchestrator).

Unlike log-server (where uvicorn already installs SIGTERM/SIGINT handlers
for a graceful ASGI shutdown), a bare `asyncio.run(...)` here would not
run any cleanup on SIGTERM by default — Python only special-cases SIGINT
(as `KeyboardInterrupt`); SIGTERM just kills the process. Since this is the
signal `docker stop`/s6-overlay's shutdown sequence actually sends, this
module installs its own handler so spec §10's graceful-shutdown steps
(cancel tasks, kill listener subprocesses, flush the logstash buffer) run
before exit instead of being skipped.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

import structlog
from log_sump_common.config import load_settings
from redis.asyncio import Redis

from .app import run
from .logging_setup import build_records_logger, close_records_logger, configure_logging

logger = structlog.get_logger(__name__)


async def _run_until_signal() -> None:
    configure_logging()
    settings = load_settings()
    records_logger = build_records_logger(
        logstash_host=settings.logstash.host,
        logstash_port=settings.logstash.port,
        database_path=settings.logstash.database_path,
    )
    # The one place log-listener talks to Redis directly -- watching the
    # runtime daemon registry (migration plan Phase 3), not shipping
    # records (that's still exclusively through records_logger above). See
    # daemon_registry.py's module docstring for why this is the narrow
    # exception rather than a new pattern to spread further.
    redis = Redis.from_url(settings.redis.url)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    run_task = asyncio.create_task(run(settings, records_logger, redis))
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        if not stop_task.done():
            stop_task.cancel()
        if not run_task.done():
            await logger.ainfo("app.shutdown_signal_received")
            run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
        # container-listener tasks already killed their `docker logs -f`
        # subprocesses on cancellation (Transport.stream_lines's own
        # guarantee) -- this just makes sure buffered records get shipped
        # (or safely persisted) before the process actually exits.
        close_records_logger()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run_until_signal())


if __name__ == "__main__":
    main()
