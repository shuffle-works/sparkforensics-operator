"""
A stand-in "SSH host" that is this machine: every command SSHAnalyzeHook or
SSHRemoteJobTrigger would send over SSH runs in a real POSIX `sh -c` (as an
SSH server hands it to the login shell), in a scratch home directory, with a
fake sparkforensics-analyze on PATH. So the provider's detached-job wrapper,
the sweep, coreutils `timeout` and the shell quoting all run for real; only
the network hop is skipped.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import textwrap
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

SAMPLE_JSON = {
    "schemaVersion": 3,
    "summary": {"impactBandCounts": {"critical": 1, "warning": 0, "info": 2}},
    "findings": [{"id": "spill", "impactBand": "critical"}],
    "recommendations": [{"id": "raise-partitions"}],
    "cleanChecks": [],
}

needs_posix_host = pytest.mark.skipif(
    not all(shutil.which(tool) for tool in ("sh", "bash", "setsid", "timeout")),
    reason="needs sh, bash, setsid and coreutils timeout, as on the SSH host",
)


class LocalHost:
    def __init__(self, root: Path):
        self.home = root / "home"
        self.bin_dir = root / "bin"
        self.home.mkdir()
        self.bin_dir.mkdir()
        self.env = {**os.environ, "HOME": str(self.home), "PATH": f"{self.bin_dir}:{os.environ['PATH']}"}
        self.connections = 0
        self.fail_connect: Exception | None = None
        # Commands containing this string raise instead of running, like a
        # connection dropping mid-session.
        self.fail_exec_matching: str | None = None

    def fake_cli(self, body: str, name: str = "sparkforensics-analyze") -> Path:
        """Installs a fake CLI. body is sh; $out is the --out value (empty
        without --out), and $argv_file has one argument per line."""
        path = self.bin_dir / name
        argv_file = self.home / "argv"
        path.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$@\" > {argv_file}\n"
            'out=""; prev=""\n'
            'for a in "$@"; do [ "$prev" = "--out" ] && out="$a"; prev="$a"; done\n'
            "emit() { if [ -n \"$out\" ]; then cat > \"$out\"; else cat; fi; }\n"
            + textwrap.dedent(body)
        )
        path.chmod(0o755)
        return path

    def argv(self) -> list[str]:
        return (self.home / "argv").read_text().splitlines()

    def report_json(self) -> str:
        return json.dumps(SAMPLE_JSON)

    # paramiko-like client, for SSHHook.get_conn()

    def _run(self, command: str, timeout=None) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["sh", "-c", command], cwd=self.home, env=self.env, capture_output=True, timeout=timeout
        )

    @contextmanager
    def patched(self):
        """Routes SSHHook and SSHRemoteJobTrigger to this host."""
        host = self

        class _Client:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def exec_command(self, command, timeout=None):
                if host.fail_exec_matching and host.fail_exec_matching in command:
                    raise EOFError("connection dropped")
                proc = host._run(command, timeout=timeout)
                stdout = MagicMock()
                stdout.channel = _Channel(proc.stdout, proc.stderr, proc.returncode)
                return MagicMock(), stdout, MagicMock()

        def _get_conn():
            host.connections += 1
            if host.fail_connect is not None:
                raise host.fail_connect
            return _Client()

        ssh_hook = MagicMock()
        ssh_hook.get_conn.side_effect = _get_conn

        async def _connect(_trigger):
            return _AsyncConn(host)

        with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook), patch(
            "airflow.providers.ssh.triggers.ssh_remote_job.SSHRemoteJobTrigger._connect", _connect
        ):
            yield


class _Channel:
    def __init__(self, stdout: bytes, stderr: bytes, exit_status: int):
        self._stdout = [stdout] if stdout else []
        self._stderr = [stderr] if stderr else []
        self._exit_status = exit_status

    def recv_ready(self):
        return bool(self._stdout)

    def recv(self, _n):
        return self._stdout.pop(0)

    def recv_stderr_ready(self):
        return bool(self._stderr)

    def recv_stderr(self, _n):
        return self._stderr.pop(0)

    def exit_status_ready(self):
        return True

    def recv_exit_status(self):
        return self._exit_status

    def shutdown_write(self):
        pass

    def close(self):
        pass


class _AsyncConn:
    """asyncssh-like connection for SSHRemoteJobTrigger."""

    def __init__(self, host: LocalHost):
        self.host = host

    async def run(self, command, timeout=None, check=False):
        proc = await asyncio.create_subprocess_exec(
            "sh", "-c", command, cwd=self.host.home, env=self.host.env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        return SimpleNamespace(
            stdout=stdout.decode(), stderr=stderr.decode(), exit_status=proc.returncode
        )

    def close(self):
        pass

    async def wait_closed(self):
        pass


def run_trigger(trigger, timeout: float = 30) -> dict:
    """Runs a trigger to its first event, as the triggerer would, and
    returns the event payload."""

    async def _first():
        async for event in trigger.run():
            return event.payload

    return asyncio.run(asyncio.wait_for(_first(), timeout))


def ti_context(try_number: int = 1, map_index: int = -1, run_id: str = "manual__2026-01-01") -> dict:
    ti = SimpleNamespace(
        dag_id="spark_dag", task_id="forensics", run_id=run_id, try_number=try_number, map_index=map_index
    )
    return {"ti": ti}


def wait_until(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); it's gone for our purposes.
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ")[-1].split()[0] != "Z"
    except FileNotFoundError:
        return True
