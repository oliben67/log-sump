"""Optional router plugins: log-sump's own generic extension point for
whatever a deployment needs beyond the built-in API surface. log-sump has
no idea what clients exist beyond "whatever talks its native API" -- a
plugin is how one of them adds its own routes without log-sump's own
package ever needing to know that client, or its API shape, exists.

A plugin is a directory under `Settings.plugins.directory` containing an
`__init__.py` that exposes a module-level `router: APIRouter`. Loaded as a
real Python package (not a single exec'd file, unlike `transforms.py`'s
one-function-per-file convention) so a plugin can freely split itself
across multiple internal modules and use ordinary relative imports between
them -- a plugin is typically a whole feature area, not one function.

Plugins run with full access to log-sump's own internals (`deps`,
`queries`, `common.config`, ...) via normal `import log_sump...` -- they
execute in this same process, not sandboxed in any way. Only point
`Settings.plugins.directory` at a directory you trust.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import structlog
from fastapi import APIRouter

logger = structlog.get_logger(__name__)


def load_plugin_routers(directory: Path) -> list[tuple[str, APIRouter]]:
    """Scans `directory` for plugin packages, returning `(name, router)`
    for every one that actually resolves to a real `APIRouter`. A plugin
    that fails to import, or doesn't expose `router` at all, is logged and
    skipped -- one broken or misconfigured plugin must never take the rest
    of the process down with it, matching every other best-effort loop in
    this codebase (`system_stats`, `containers_listing`, ...).
    """
    routers: list[tuple[str, APIRouter]] = []
    if not directory.is_dir():
        return routers
    for entry in sorted(directory.iterdir()):
        if entry.name.startswith("_") or not entry.is_dir():
            continue
        init_file = entry / "__init__.py"
        if not init_file.is_file():
            continue
        try:
            router = _load_plugin_router(entry.name, init_file, entry)
        except Exception as exc:  # noqa: BLE001 -- a broken plugin must not crash startup
            logger.error("plugins.load_failed", plugin=entry.name, error=str(exc))
            continue
        if router is None:
            logger.warning("plugins.no_router", plugin=entry.name)
            continue
        routers.append((entry.name, router))
        logger.info("plugins.loaded", plugin=entry.name)
    return routers


def _load_plugin_router(name: str, init_file: Path, package_dir: Path) -> APIRouter | None:
    module_name = f"log_sump_plugin_{name}"
    spec = importlib.util.spec_from_file_location(
        module_name, init_file, submodule_search_locations=[str(package_dir)]
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load plugin {name!r} from {init_file}")
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so the plugin's own internal relative imports
    # (e.g. `from . import routes`) resolve it as an already-known package,
    # not a bare unregistered module mid-import.
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    router = getattr(module, "router", None)
    return router if isinstance(router, APIRouter) else None
