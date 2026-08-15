"""CLI entrypoint: `python -m log_sump.server`."""

from __future__ import annotations

import uvicorn

from log_sump.common.config import load_settings

from .app import create_app


def main() -> None:
    settings = load_settings()
    app = create_app(settings)
    # Built manually (not the `uvicorn.run()` convenience wrapper) so
    # `POST /shutdown` (migration plan Phase 7, `routers/gateway.py`) has a
    # `Server` instance to set `should_exit` on.
    config = uvicorn.Config(app, host=settings.server.bind_host, port=settings.server.bind_port)
    server = uvicorn.Server(config)
    app.state.uvicorn_server = server
    server.run()


if __name__ == "__main__":
    main()
