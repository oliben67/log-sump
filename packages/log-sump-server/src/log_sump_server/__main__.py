"""CLI entrypoint: `python -m log_sump_server`."""

from __future__ import annotations

import uvicorn
from log_sump_common.config import load_settings

from .app import create_app


def main() -> None:
    settings = load_settings()
    app = create_app(settings)
    uvicorn.run(app, host=settings.server.bind_host, port=settings.server.bind_port)


if __name__ == "__main__":
    main()
