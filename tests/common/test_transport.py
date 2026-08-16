import asyncio
import time

import pytest

from log_sump.common import transport as transport_module
from log_sump.common.transport import LocalTransport, SSHTransport, StreamLine, TransportError

# ── fakes for SSHTransport's paramiko usage (no real network involved) ──────


class _FakeStdout:
    def __init__(self, data: bytes, exit_status: int) -> None:
        self._data = data
        self.channel = _FakeExecChannel(exit_status=exit_status)

    def read(self) -> bytes:
        return self._data


class _FakeStderr:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeExecChannel:
    """Just enough of a paramiko exec-command channel for `_run_sync`."""

    def __init__(self, exit_status: int) -> None:
        self._exit_status = exit_status

    def recv_exit_status(self) -> int:
        return self._exit_status


class _FakeStreamChannel:
    """Enough of a paramiko streaming channel for `stream_lines`'s read loop --
    `recv_ready`/`recv_stderr_ready` report whether a queued chunk remains
    (real paramiko's actual readiness signal), and `exit_status_ready`
    flips once both queues are drained, ending the loop the same way a
    real remote command finishing would.
    """

    def __init__(self, stdout_chunks: list[bytes], stderr_chunks: list[bytes]) -> None:
        self._stdout_chunks = list(stdout_chunks)
        self._stderr_chunks = list(stderr_chunks)

    def fileno(self) -> int:
        return -1  # unused -- the fake select.select below never calls this

    def setblocking(self, _value: int) -> None:
        pass

    def recv_ready(self) -> bool:
        return bool(self._stdout_chunks)

    def recv(self, _n: int) -> bytes:
        return self._stdout_chunks.pop(0) if self._stdout_chunks else b""

    def recv_stderr_ready(self) -> bool:
        return bool(self._stderr_chunks)

    def recv_stderr(self, _n: int) -> bytes:
        return self._stderr_chunks.pop(0) if self._stderr_chunks else b""

    def exit_status_ready(self) -> bool:
        return not self._stdout_chunks and not self._stderr_chunks


class _FakeSSHClient:
    def __init__(
        self,
        *,
        exec_result: tuple[bytes, bytes, int] = (b"", b"", 0),
        connect_error: Exception | None = None,
        stream_channel: _FakeStreamChannel | None = None,
    ) -> None:
        self._exec_result = exec_result
        self._connect_error = connect_error
        self._stream_channel = stream_channel
        self.connect_kwargs: dict[str, object] | None = None
        self.exec_commands: list[str] = []
        self.closed = False

    def set_missing_host_key_policy(self, _policy: object) -> None:
        pass

    def connect(self, host: str, **kwargs: object) -> None:
        if self._connect_error is not None:
            raise self._connect_error
        self.connect_kwargs = {"host": host, **kwargs}

    def exec_command(self, command: str):
        self.exec_commands.append(command)
        if self._stream_channel is not None:
            stdout = type("_S", (), {"channel": self._stream_channel})()
            return None, stdout, None
        out, err, code = self._exec_result
        return None, _FakeStdout(out, code), _FakeStderr(err)

    def close(self) -> None:
        self.closed = True


def _patch_ssh_client(monkeypatch: pytest.MonkeyPatch, client: _FakeSSHClient) -> None:
    monkeypatch.setattr(transport_module.paramiko, "SSHClient", lambda: client)


def _patch_select_always_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    # The real select.select needs real file descriptors; _FakeStreamChannel
    # has none, so its own recv_ready()/recv_stderr_ready() already carry
    # the "is there anything to read" signal -- this fake just reports the
    # channel as immediately readable every poll, letting SSHTransport's
    # real read loop drive itself off the fake channel's own state.
    monkeypatch.setattr(
        transport_module.select, "select", lambda rlist, wlist, xlist, timeout: (rlist, [], [])
    )


# ── SSHTransport ─────────────────────────────────────────────────────────


def test_ssh_transport_shell_command_is_a_single_argument() -> None:
    # A single trailing argument is what makes paramiko's exec_command hand
    # it to the remote shell as-is, unsplit -- see
    # Transport._shell_command's docstring.
    transport = SSHTransport(host="10.0.0.5", user="deploy")
    assert transport._shell_command("cat a; cat b") == ["cat a; cat b"]


async def test_ssh_transport_run_connects_with_host_user_port_and_returns_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeSSHClient(exec_result=(b"hello\n", b"", 0))
    _patch_ssh_client(monkeypatch, client)

    result = await SSHTransport(host="10.0.0.5", user="deploy", port=2222).run(
        ["docker", "ps", "--format", "{{json .}}"]
    )

    assert result.returncode == 0
    assert result.stdout == "hello\n"
    assert client.connect_kwargs == {
        "host": "10.0.0.5",
        "port": 2222,
        "username": "deploy",
        "allow_agent": True,
        "look_for_keys": True,
        "timeout": 10,
    }
    # shlex-joined into one command string, quoting the value with the
    # embedded space -- not four separate remote-shell words.
    assert client.exec_commands == ["docker ps --format '{{json .}}'"]
    assert client.closed, "the connection must not be left open after run()"


async def test_ssh_transport_run_reports_nonzero_exit_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeSSHClient(exec_result=(b"", b"no such container\n", 1))
    _patch_ssh_client(monkeypatch, client)

    result = await SSHTransport(host="10.0.0.5", user="deploy").run(["docker", "ps"])
    assert result.returncode == 1
    assert "no such container" in result.stderr
    with pytest.raises(TransportError):
        result.check()


async def test_ssh_transport_run_reports_connect_failure_as_a_failed_result_not_an_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Matches subprocess-based Transport's existing contract: a failed
    # *connection* looks the same as a failed *command* to callers -- a
    # non-zero ExecResult, never a raised exception from run() itself, so
    # every existing `except TransportError` call site keeps working
    # whether the failure happened at the ssh layer or the docker layer.
    client = _FakeSSHClient(connect_error=OSError("connection refused"))
    _patch_ssh_client(monkeypatch, client)

    result = await SSHTransport(host="10.0.0.5", user="deploy").run(["docker", "ps"])
    assert result.returncode != 0
    assert "connection refused" in result.stderr


async def test_ssh_transport_stream_lines_yields_stdout_and_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = _FakeStreamChannel(
        stdout_chunks=[b"out-1\nout-2\n"], stderr_chunks=[b"err-1\n"]
    )
    client = _FakeSSHClient(stream_channel=channel)
    _patch_ssh_client(monkeypatch, client)
    _patch_select_always_ready(monkeypatch)

    seen: list[StreamLine] = []
    async with SSHTransport(host="10.0.0.5", user="deploy").stream_lines(
        ["docker", "logs", "-f", "c1"]
    ) as lines:
        async for line in lines:
            seen.append(line)

    pairs = [(item.stream, item.text) for item in seen]
    assert ("stdout", "out-1") in pairs
    assert ("stdout", "out-2") in pairs
    assert ("stderr", "err-1") in pairs
    assert client.closed, "stream_lines must close the ssh connection when the block exits"


async def test_ssh_transport_stream_lines_closes_client_on_early_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A never-ending stream (like `docker logs -f` against a live
    # container): the fake channel always has more "stdout" ready, so the
    # only way this test finishes is if breaking out of the `async with`
    # block actually stops the worker thread instead of it looping forever.
    class _InfiniteChannel(_FakeStreamChannel):
        def recv_ready(self) -> bool:
            return True

        def recv(self, _n: int) -> bytes:
            return b"line\n"

        def exit_status_ready(self) -> bool:
            return False

    channel = _InfiniteChannel(stdout_chunks=[], stderr_chunks=[])
    client = _FakeSSHClient(stream_channel=channel)
    _patch_ssh_client(monkeypatch, client)
    _patch_select_always_ready(monkeypatch)

    start = time.monotonic()
    transport = SSHTransport(host="10.0.0.5", user="deploy")
    async with transport.stream_lines(["docker", "logs", "-f", "c1"]) as lines:
        async for _line in lines:
            break
    elapsed = time.monotonic() - start

    assert elapsed < 3.0, "stream_lines should stop the worker thread, not wait on it forever"
    assert client.closed


# ── LocalTransport (unchanged: still subprocess-based) ──────────────────


def test_local_transport_shell_command_wraps_with_sh_c() -> None:
    # Local exec has no implicit shell (unlike ssh's remote command
    # handling), so this needs its own explicit sh -c wrapper.
    assert LocalTransport()._shell_command("cat a; cat b") == ["sh", "-c", "cat a; cat b"]


async def test_run_shell_actually_executes_a_compound_command() -> None:
    # The real regression test: if LocalTransport._shell_command ever
    # regressed to `[script]` (the SSH shape) instead of `["sh", "-c",
    # script]`, this would fail outright -- create_subprocess_exec would try
    # to execute a file literally named "echo one; echo two".
    result = await LocalTransport().run_shell("echo one; echo two")
    result.check()
    assert result.stdout.splitlines() == ["one", "two"]


async def test_run_captures_stdout_and_exit_code() -> None:
    result = await LocalTransport().run(["python3", "-c", "print('hello')"])
    assert result.returncode == 0
    assert result.stdout.strip() == "hello"


async def test_run_check_raises_on_nonzero_exit() -> None:
    result = await LocalTransport().run(["python3", "-c", "import sys; sys.exit(3)"])
    assert result.returncode == 3
    with pytest.raises(TransportError):
        result.check()


async def test_run_cancellation_via_asyncio_timeout_kills_subprocess() -> None:
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await LocalTransport().run(["python3", "-c", "import time; time.sleep(5)"])
    assert time.monotonic() - start < 2.0


async def test_stream_lines_yields_stdout_and_stderr() -> None:
    script = "import sys; print('out-1'); print('err-1', file=sys.stderr); print('out-2')"
    seen: list[tuple[str, str]] = []
    async with LocalTransport().stream_lines(["python3", "-c", script]) as lines:
        async for line in lines:
            seen.append((line.stream, line.text))

    assert ("stdout", "out-1") in seen
    assert ("stdout", "out-2") in seen
    assert ("stderr", "err-1") in seen


async def test_stream_lines_kills_subprocess_on_early_exit() -> None:
    script = "import time\nfor i in range(30):\n    print(i, flush=True)\n    time.sleep(1)\n"
    start = time.monotonic()
    async with LocalTransport().stream_lines(["python3", "-c", script]) as lines:
        async for _line in lines:
            break
    elapsed = time.monotonic() - start
    assert elapsed < 3.0, (
        "stream_lines should kill the subprocess instead of waiting for it to finish"
    )
