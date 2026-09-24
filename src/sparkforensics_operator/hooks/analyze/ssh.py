from __future__ import annotations

import shlex
import time

from airflow.exceptions import AirflowException

from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, RemoteEventLog
from sparkforensics_operator.report import Report

from ._cli import build_cli_args, build_report, check_exit_code
from .base import AnalyzeHook

# POSIX shells exit 127 for "command not found" and 126 for "found but not
# executable".
_SHELL_COMMAND_NOT_FOUND_EXIT_CODES = {126, 127}
# coreutils `timeout` exits 124 when it had to stop the command.
_REMOTE_TIMEOUT_EXIT_CODE = 124
_READ_CHUNK_BYTES = 1024 * 1024
_POLL_INTERVAL_S = 0.1


class SSHAnalyzeHook(AnalyzeHook):
    """Runs `sparkforensics-analyze ... --format json [threshold flags]` on
    the host behind ssh_conn_id (the same Airflow connection SSHOperator,
    SFTPLogSourceHook and SSHTunneledLogSourceHook use) and reads the JSON
    report from its stdout. The event log never crosses the network, and the
    Airflow worker needs neither Node.js nor the CLI: that host needs
    Node.js 18+ and the sparkforensics-cli npm package. Reads a
    RemoteEventLog on the same ssh_conn_id (RemotePathLogSourceHook) or a
    HistoryServerApp (HistoryServerAppLogSourceHook), whose base_url is
    resolved on that host, e.g. http://localhost:18080. Requires the `ssh`
    extra."""

    supported_log_refs = (RemoteEventLog, HistoryServerApp)

    def __init__(self, ssh_conn_id: str, analyze_bin: str = "sparkforensics-analyze", timeout: int = 900):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.analyze_bin = analyze_bin
        self.timeout = timeout

    @property
    def _where(self) -> str:
        return f" on the SSH host (ssh_conn_id={self.ssh_conn_id!r})"

    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        if isinstance(log_ref, RemoteEventLog) and log_ref.ssh_conn_id != self.ssh_conn_id:
            raise AirflowException(
                f"SSHAnalyzeHook(ssh_conn_id={self.ssh_conn_id!r}) cannot analyze an event log "
                f"on a different SSH host ({log_ref.describe()}). Use the same ssh_conn_id "
                "for the log source and the backend."
            )

        # shlex.join quotes every argument for the remote POSIX shell, so a
        # rendered path, app id or analyze_bin can't inject shell syntax.
        # Closing a non-PTY channel doesn't signal the remote process, so
        # coreutils `timeout` bounds it on the host itself.
        command = shlex.join(
            ["timeout", str(self.timeout), *build_cli_args(self.analyze_bin, log_ref, thresholds)]
        )
        returncode, stdout, stderr = self._run_remote(command, log_ref)

        if returncode == _REMOTE_TIMEOUT_EXIT_CODE:
            raise self._timeout_error(log_ref)
        if returncode in _SHELL_COMMAND_NOT_FOUND_EXIT_CODES:
            raise AirflowException(
                f"sparkforensics-analyze binary not found or not executable{self._where}: "
                f"{self.analyze_bin!r} or the coreutils `timeout` it runs under (exit {returncode}). "
                "Check the stderr below for which one. Install the CLI on that host with "
                "`npm install -g sparkforensics-cli` (Node.js 18+), or pass "
                "analyze_bin=<full path to sparkforensics-analyze>: a non-interactive SSH "
                "session may not load the PATH a login shell sets (e.g. for nvm). "
                f"{stderr.strip()}"
            )
        check_exit_code(returncode, stderr, self._where)
        return build_report(stdout, thresholds, stderr, returncode, self._where)

    def _timeout_error(self, log_ref: EventLogRef) -> AirflowException:
        return AirflowException(
            f"sparkforensics-analyze timed out after {self.timeout}s"
            f"{self._where} analyzing {log_ref.describe()}."
        )

    def _run_remote(self, command: str, log_ref: EventLogRef) -> tuple[int, str, str]:
        """Returns (exit status, stdout, stderr). Reads the channel directly
        rather than through SSHHook.exec_ssh_client_command, which bounds
        only each idle read (not the whole run) and logs every stdout line,
        i.e. the entire JSON report, to the task log."""
        from airflow.providers.ssh.hooks.ssh import SSHHook

        try:
            ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            with ssh_hook.get_conn() as client:
                stdin, stdout, _ = client.exec_command(command, timeout=self.timeout)
                channel = stdout.channel
                stdin.close()
                channel.shutdown_write()

                out_chunks: list[bytes] = []
                err_chunks: list[bytes] = []
                deadline = time.monotonic() + self.timeout
                while True:
                    received = False
                    while channel.recv_ready():
                        out_chunks.append(channel.recv(_READ_CHUNK_BYTES))
                        received = True
                    while channel.recv_stderr_ready():
                        err_chunks.append(channel.recv_stderr(_READ_CHUNK_BYTES))
                        received = True
                    if channel.exit_status_ready() and not (
                        channel.recv_ready() or channel.recv_stderr_ready()
                    ):
                        break
                    if time.monotonic() > deadline:
                        channel.close()
                        raise self._timeout_error(log_ref)
                    if not received:
                        time.sleep(_POLL_INTERVAL_S)
                returncode = channel.recv_exit_status()
        except AirflowException:
            raise
        except Exception as e:
            raise AirflowException(
                f"SSH remote analysis failed (ssh_conn_id={self.ssh_conn_id!r}, "
                f"command={command!r}): {e}"
            ) from e

        return (
            returncode,
            b"".join(out_chunks).decode("utf-8", "replace"),
            b"".join(err_chunks).decode("utf-8", "replace"),
        )
