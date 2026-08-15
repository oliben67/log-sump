"""Shared test doubles for log_sump.listener tests."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Sequence

from log_sump.common.transport import ExecResult, StreamLine, Transport


class FakeTransport(Transport):
    """Replays a fixed list of `StreamLine`s instead of spawning a subprocess.

    After replaying, the generator stays open — like a real `docker logs -f`
    still tailing — so tests can exercise cancellation. Pass `raise_on_start`
    to simulate a transport/connection failure instead.
    """

    def __init__(
        self,
        lines: Sequence[StreamLine] = (),
        *,
        raise_on_start: Exception | None = None,
        run_result: ExecResult | None = None,
    ) -> None:
        self._lines = list(lines)
        self._raise_on_start = raise_on_start
        self._run_result = run_result

    def command_prefix(self) -> list[str]:
        return []

    def _shell_command(self, script: str) -> list[str]:
        return ["sh", "-c", script]

    async def run(self, args: Sequence[str]) -> ExecResult:
        if self._raise_on_start is not None:
            raise self._raise_on_start
        assert self._run_result is not None
        return self._run_result

    @contextlib.asynccontextmanager
    async def stream_lines(self, args: Sequence[str]) -> AsyncIterator[AsyncIterator[StreamLine]]:
        if self._raise_on_start is not None:
            raise self._raise_on_start

        async def generator() -> AsyncGenerator[StreamLine, None]:
            for line in self._lines:
                yield line
            await asyncio.Event().wait()  # stay open, like a real `-f` tail

        gen: AsyncGenerator[StreamLine, None] = generator()
        try:
            yield gen
        finally:
            await gen.aclose()


class FakeRecordsLogger:
    """Duck-typed stand-in for the `structlog.stdlib.BoundLogger` records logger."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ainfo(self, event: str) -> None:
        self.calls.append(event)
