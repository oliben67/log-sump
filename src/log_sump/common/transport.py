"""Daemon transport interface.

Every command that reaches a Docker daemon's host (`docker ps`, `docker logs
-f`, `docker stats --no-stream`, `docker system df`, `cat /proc/...`) goes
through a `Transport`. The spec's assumed default is the Docker CLI reached
over SSH (`ssh <user>@<host> docker ...`), run via
`asyncio.create_subprocess_exec` so the event loop is never blocked — but
callers only ever depend on this interface, so a future transport (e.g.
aiodocker over a TLS socket, per spec §11.1) can swap in without touching
call sites.

`SSHTransport` and `LocalTransport` share all subprocess plumbing (spawn,
line-buffered stdout/stderr reads, guaranteed cleanup on cancellation) via
the `Transport` base class — they differ only in the argv prefix used to
route a command to the right place.
"""

from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Literal


class TransportError(RuntimeError):
    """Raised when a transport-level command fails or the host is unreachable."""


@dataclass(frozen=True)
class ExecResult:
    returncode: int
    stdout: str
    stderr: str

    def check(self) -> ExecResult:
        if self.returncode != 0:
            raise TransportError(f"command exited {self.returncode}: {self.stderr.strip()}")
        return self


@dataclass(frozen=True)
class StreamLine:
    stream: Literal["stdout", "stderr"]
    text: str


class Transport(ABC):
    """Runs Docker CLI / shell commands against one daemon's host."""

    @abstractmethod
    def command_prefix(self) -> list[str]:
        """Argv prefix prepended to every command, e.g. `["ssh", "user@host"]`."""

    @abstractmethod
    def _shell_command(self, script: str) -> list[str]:
        """Wrap a compound shell script (`;`, pipes, ...) into an argv list.

        A plain argv list has no shell to interpret `;`/pipes, and how to
        invoke one differs by transport in a way that's easy to get subtly
        wrong: OpenSSH joins *all* trailing arguments after the destination
        into one string and hands that to the remote shell, so
        `ssh host sh -c "cat a; cat b"` (passed as three separate argv
        elements `"sh"`, `"-c"`, `"cat a; cat b"`) gets rejoined into
        `sh -c cat a; cat b` remotely — silently splitting our script at the
        `;` instead of running it as one `sh -c` invocation. Passing the
        whole script as a *single* argument after the destination sidesteps
        that rejoining. Local execution has no implicit shell at all (there's
        nothing to route the command through, unlike a remote sshd), so it
        needs an explicit `sh -c` wrapper instead. See `run_shell`.
        """

    async def run_shell(self, script: str) -> ExecResult:
        """Run a compound shell command (supports `;`, pipes, redirection, ...)."""
        return await self.run(self._shell_command(script))

    async def run(self, args: Sequence[str]) -> ExecResult:
        """Run a one-shot command (e.g. `docker ps`) and return its full output.

        Takes no `timeout` param by design — wrap the call in
        `async with asyncio.timeout(seconds):` instead. That way cancellation
        (whether from a timeout or an outer shutdown) always follows the same
        path below, which guarantees the subprocess gets killed rather than
        leaked, instead of only doing so for one specific timeout mechanism.
        """
        argv = [*self.command_prefix(), *args]
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            raise
        return ExecResult(
            returncode=proc.returncode or 0,
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
        )

    @contextlib.asynccontextmanager
    async def stream_lines(self, args: Sequence[str]) -> AsyncIterator[AsyncIterator[StreamLine]]:
        """Run a long-lived command and yield its stdout/stderr, line by line.

        Usage::

            async with transport.stream_lines(args) as lines:
                async for line in lines:
                    ...

        Guarantees the subprocess is terminated on cancellation or normal
        exit from the `async with` block, so callers never leak an
        `ssh`/`docker logs -f` child process.
        """
        argv = [*self.command_prefix(), *args]
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert proc.stdout is not None
        assert proc.stderr is not None
        lines: AsyncGenerator[StreamLine, None] = _merge_streams(proc.stdout, proc.stderr)
        try:
            yield lines
        finally:
            await lines.aclose()
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


async def _merge_streams(
    stdout: asyncio.StreamReader, stderr: asyncio.StreamReader
) -> AsyncGenerator[StreamLine, None]:
    """Interleave two `StreamReader`s as they produce lines, in arrival order."""
    queue: asyncio.Queue[StreamLine | None] = asyncio.Queue()

    async def pump(stream: asyncio.StreamReader, name: Literal["stdout", "stderr"]) -> None:
        async for raw_line in stream:
            await queue.put(
                StreamLine(stream=name, text=raw_line.decode(errors="replace").rstrip("\n"))
            )
        await queue.put(None)

    pumps = [
        asyncio.create_task(pump(stdout, "stdout")),
        asyncio.create_task(pump(stderr, "stderr")),
    ]
    remaining = len(pumps)
    try:
        while remaining > 0:
            item = await queue.get()
            if item is None:
                remaining -= 1
                continue
            yield item
    finally:
        for task in pumps:
            task.cancel()
        await asyncio.gather(*pumps, return_exceptions=True)


class SSHTransport(Transport):
    """Reaches a daemon as `ssh <user>@<host> <command>` (spec §3 assumption #1)."""

    def __init__(self, host: str, user: str, *, ssh_options: Sequence[str] = ()) -> None:
        self._host = host
        self._user = user
        self._ssh_options = list(ssh_options)

    def command_prefix(self) -> list[str]:
        return ["ssh", *self._ssh_options, f"{self._user}@{self._host}"]

    def _shell_command(self, script: str) -> list[str]:
        # A single trailing argument after the destination -- ssh hands it
        # to the remote shell as-is, with nothing to (mis)join. See
        # `Transport._shell_command` for why this must NOT be `["sh", "-c",
        # script]` here.
        return [script]


class LocalTransport(Transport):
    """Runs commands directly on the local machine — no `ssh` wrapper.

    Used for `transport: local` daemons so the exact same registry/tracker/
    listener/stats code paths run against a Docker socket already on this
    machine, with no SSH setup required. Originally added for local dev/
    tests; now also the supported choice for an embedded "This machine"
    gateway target (migration plan Phase 3) — no code changed to get there,
    once Phases 1/2 had exercised every code path this transport also
    exercises against the same production query/ingestion surface.
    """

    def command_prefix(self) -> list[str]:
        return []

    def _shell_command(self, script: str) -> list[str]:
        # No ssh (and so no implicit remote shell) in the picture locally --
        # this process has to invoke one itself.
        return ["sh", "-c", script]
