"""Daemon transport interface.

Every command that reaches a Docker daemon's host (`docker ps`, `docker logs
-f`, `docker stats --no-stream`, `docker system df`, `cat /proc/...`) goes
through a `Transport`. `SSHTransport` reaches a remote daemon over `paramiko`
(a pure-Python SSH client) — not the system `ssh` binary, and not any
Docker-specific transport (`docker -H ssh://`, a Docker SDK) — so every
command it runs is a literal `docker ...`/`cat ...` invocation over a plain
SSH session, and the only new operational requirement it adds is this
package's own `paramiko` dependency, not an `ssh` binary on whatever host
runs `log-listener`. `LocalTransport` still runs commands directly via
`asyncio.create_subprocess_exec`, so the event loop is never blocked either
way — but callers only ever depend on this interface, so a future transport
(e.g. aiodocker over a TLS socket, per spec §11.1) can swap in without
touching call sites.
"""

from __future__ import annotations

import asyncio
import contextlib
import select
import shlex
import threading
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Literal

import paramiko


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
    def _shell_command(self, script: str) -> list[str]:
        """Wrap a compound shell script (`;`, pipes, ...) into an argv list.

        A plain argv list has no shell to interpret `;`/pipes, and how to
        invoke one differs by transport in a way that's easy to get subtly
        wrong: a remote shell (SSH) already receives one joined command
        string, so `sh -c "cat a; cat b"` (passed as three separate argv
        elements `"sh"`, `"-c"`, `"cat a; cat b"`) would get rejoined into
        `sh -c cat a; cat b` remotely — silently splitting our script at the
        `;` instead of running it as one `sh -c` invocation. Passing the
        whole script as a *single* argument sidesteps that rejoining. Local
        execution has no implicit shell at all (there's nothing to route
        the command through), so it needs an explicit `sh -c` wrapper
        instead. See `run_shell`.
        """

    @abstractmethod
    async def run(self, args: Sequence[str]) -> ExecResult:
        """Run a one-shot command (e.g. `docker ps`) and return its full output.

        Takes no `timeout` param by design — wrap the call in
        `async with asyncio.timeout(seconds):` instead. That way cancellation
        (whether from a timeout or an outer shutdown) always follows the same
        path, which guarantees the underlying command gets killed rather
        than leaked, instead of only doing so for one specific timeout
        mechanism.
        """

    @abstractmethod
    def stream_lines(
        self, args: Sequence[str]
    ) -> contextlib.AbstractAsyncContextManager[AsyncIterator[StreamLine]]:
        """Run a long-lived command and yield its stdout/stderr, line by line.

        Usage::

            async with transport.stream_lines(args) as lines:
                async for line in lines:
                    ...

        Guarantees the underlying command is terminated on cancellation or
        normal exit from the `async with` block, so callers never leak an
        `ssh` session / `docker logs -f` process.
        """

    async def run_shell(self, script: str) -> ExecResult:
        """Run a compound shell command (supports `;`, pipes, redirection, ...)."""
        return await self.run(self._shell_command(script))


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

    def _shell_command(self, script: str) -> list[str]:
        # No ssh (and so no implicit remote shell) in the picture locally --
        # this process has to invoke one itself.
        return ["sh", "-c", script]

    async def run(self, args: Sequence[str]) -> ExecResult:
        proc = await asyncio.create_subprocess_exec(
            *args,
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
        proc = await asyncio.create_subprocess_exec(
            *args,
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


#: How often the stream_lines() background thread re-checks for
#: cancellation between reads -- bounds how long a cancelled `async with`
#: block can take to actually notice the worker thread has stopped.
_SSH_POLL_INTERVAL_S = 0.5
#: `run()`/`stream_lines()` never raise on a connection/command failure --
#: they report it the same way a failed `ssh` *process* would (a non-zero
#: exit code / a stderr line), matching subprocess-based Transport's
#: existing contract so callers don't need transport-specific except
#: clauses. 255 mirrors OpenSSH's own "couldn't establish connection" exit
#: code.
_SSH_FAILURE_RETURNCODE = 255


class SSHTransport(Transport):
    """Reaches a daemon over SSH via `paramiko` (spec §3 assumption #1) --
    a pure-Python client, not the system `ssh` binary.
    """

    def __init__(self, host: str, user: str, *, port: int = 22) -> None:
        self._host = host
        self._user = user
        self._port = port

    def _shell_command(self, script: str) -> list[str]:
        # A single command string -- paramiko's exec_command already runs
        # it through the remote login shell, so no local wrapping is
        # needed (mirrors the old ssh-CLI behavior, see
        # Transport._shell_command's docstring for why this must stay one
        # argument rather than ["sh", "-c", script]).
        return [script]

    @staticmethod
    def _with_sudo(args: Sequence[str]) -> Sequence[str]:
        """Prepend `sudo` to remote `docker` invocations (br-CONN-004).

        The account log-listener SSHes in as often isn't in the remote
        host's `docker` group -- unlike `LocalTransport`, which never needs
        this (this container has no `sudo` binary at all, and already runs
        as root against a docker.sock it owns directly). Matches the old
        `app/server/server.py::_exec_remote_docker`'s `"sudo docker " +
        ...` behavior, which this class's `run`/`stream_lines` regressed
        when the SSH path was rewritten onto paramiko.
        """
        if args and args[0] == "docker":
            return ["sudo", *args]
        return args

    def _connect(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        # Trust-on-first-use: matches app/lib/server-provision.js's own
        # StrictHostKeyChecking=accept-new for the client->gateway hop --
        # this is the equivalent policy for the gateway->docker-host hop.
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            self._host,
            port=self._port,
            username=self._user,
            # Same credential sources the old ssh-CLI-based transport relied
            # on implicitly: SSH_AUTH_SOCK agent forwarding and a mounted
            # private key file (see docker-compose.yml's SSH_AUTH_SOCK /
            # CTTC_ID_RSA-derived ~/.ssh/id_rsa mounts) -- allow_agent reads
            # SSH_AUTH_SOCK itself; look_for_keys falls back to the default
            # identity files paramiko already knows to check.
            allow_agent=True,
            look_for_keys=True,
            timeout=10,
        )
        return client

    async def run(self, args: Sequence[str]) -> ExecResult:
        command = shlex.join(self._with_sudo(args))
        return await asyncio.to_thread(self._run_sync, command)

    def _run_sync(self, command: str) -> ExecResult:
        # Reported via ExecResult, never raised -- see _SSH_FAILURE_RETURNCODE.
        try:
            client = self._connect()
        except Exception as exc:  # noqa: BLE001
            return ExecResult(
                returncode=_SSH_FAILURE_RETURNCODE, stdout="", stderr=f"ssh connect failed: {exc}"
            )
        try:
            _stdin, stdout, stderr = client.exec_command(command)
            out = stdout.read().decode(errors="replace")
            err = stderr.read().decode(errors="replace")
            code = stdout.channel.recv_exit_status()
            return ExecResult(returncode=code, stdout=out, stderr=err)
        except Exception as exc:  # noqa: BLE001
            return ExecResult(
                returncode=_SSH_FAILURE_RETURNCODE, stdout="", stderr=f"ssh command failed: {exc}"
            )
        finally:
            client.close()

    @contextlib.asynccontextmanager
    async def stream_lines(self, args: Sequence[str]) -> AsyncIterator[AsyncIterator[StreamLine]]:
        command = shlex.join(self._with_sudo(args))
        loop = asyncio.get_event_loop()
        queue: asyncio.Queue[StreamLine | None] = asyncio.Queue()
        stop_event = threading.Event()
        state: dict[str, paramiko.SSHClient] = {}

        def emit(buf: bytes, chunk: bytes, stream: Literal["stdout", "stderr"]) -> bytes:
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                item = StreamLine(stream=stream, text=line.decode(errors="replace"))
                loop.call_soon_threadsafe(queue.put_nowait, item)
            return buf

        def worker() -> None:
            try:
                client = self._connect()
            except Exception as exc:  # noqa: BLE001 -- surfaced as a stderr line, like a failed ssh process would be
                error_line = StreamLine(stream="stderr", text=f"ssh connect failed: {exc}")
                loop.call_soon_threadsafe(queue.put_nowait, error_line)
                loop.call_soon_threadsafe(queue.put_nowait, None)
                return
            state["client"] = client
            try:
                if stop_event.is_set():
                    return
                _stdin, stdout, _stderr = client.exec_command(command)
                channel = stdout.channel
                channel.setblocking(0)
                stdout_buf = b""
                stderr_buf = b""
                while not stop_event.is_set():
                    readable, _, _ = select.select([channel], [], [], _SSH_POLL_INTERVAL_S)
                    read_any = False
                    if channel in readable:
                        if channel.recv_ready():
                            chunk = channel.recv(4096)
                            if chunk:
                                read_any = True
                                stdout_buf = emit(stdout_buf, chunk, "stdout")
                        if channel.recv_stderr_ready():
                            chunk = channel.recv_stderr(4096)
                            if chunk:
                                read_any = True
                                stderr_buf = emit(stderr_buf, chunk, "stderr")
                    if not read_any and channel.exit_status_ready():
                        break
            except (OSError, EOFError, paramiko.SSHException):
                # A closed channel (our own cancellation path below closing
                # `client`) or the remote end going away mid-read -- not a
                # real failure worth surfacing, just how a forced stop looks
                # from this thread's side.
                pass
            finally:
                client.close()
                loop.call_soon_threadsafe(queue.put_nowait, None)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

        async def _lines() -> AsyncGenerator[StreamLine, None]:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item

        gen = _lines()
        try:
            yield gen
        finally:
            stop_event.set()
            client = state.get("client")
            if client is not None:
                # Closing here (not just setting stop_event) is what
                # actually unblocks a worker thread parked in select() /
                # recv() on a long-lived command like `docker logs -f`,
                # the same way killing the old ssh-CLI subprocess used to.
                client.close()
            await gen.aclose()
            await asyncio.to_thread(thread.join, 5)
