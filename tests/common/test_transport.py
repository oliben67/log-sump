import asyncio
import time

import pytest

from log_sump.common.transport import LocalTransport, SSHTransport, TransportError


def test_ssh_transport_command_prefix() -> None:
    transport = SSHTransport(host="10.0.0.5", user="deploy", ssh_options=["-o", "BatchMode=yes"])
    assert transport.command_prefix() == ["ssh", "-o", "BatchMode=yes", "deploy@10.0.0.5"]


def test_local_transport_command_prefix() -> None:
    assert LocalTransport().command_prefix() == []


def test_ssh_transport_shell_command_is_a_single_argument() -> None:
    # A single trailing argument after the destination is what makes ssh
    # hand it to the remote shell as-is. If this ever became multiple
    # elements (e.g. ["sh", "-c", script]), ssh would rejoin them with
    # spaces before the remote shell sees them, silently mangling any `;`
    # or quoting in `script` -- see Transport._shell_command's docstring.
    transport = SSHTransport(host="10.0.0.5", user="deploy")
    assert transport._shell_command("cat a; cat b") == ["cat a; cat b"]


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
