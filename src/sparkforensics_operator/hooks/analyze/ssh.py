from __future__ import annotations

import re
import time
from datetime import timedelta
from typing import Any

from airflow.exceptions import AirflowException

from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, RemoteEventLog
from sparkforensics_operator.report import Report

from . import _remote_job
from ._cli import USABLE_REPORT_EXIT_CODES, build_report, check_exit_code
from .base import DeferrableAnalyzeHook

# POSIX shells exit 127 for "command not found" and 126 for "found but not
# executable".
_SHELL_COMMAND_NOT_FOUND_EXIT_CODES = {126, 127}
# coreutils `timeout` exits 124 when it had to stop the command.
_REMOTE_TIMEOUT_EXIT_CODE = 124
_READ_CHUNK_BYTES = 1024 * 1024
_POLL_INTERVAL_S = 0.1
# Wall-clock bound on each short command of a deferrable run (sweep, submit,
# read back, clean up); the analysis itself is bounded by `timeout`.
_JOB_COMMAND_TIMEOUT_S = 120
# How long past the job's own time limit the deferral waits before giving up
# on a job that never reported back (host rebooted, wrapper killed).
_DEFER_GRACE_S = 120
_PID_LINE = re.compile(rb"^" + re.escape(_remote_job.SYNC_PID_MARKER.encode()) + rb"(\d+)\n", re.M)


def _where(ssh_conn_id: str) -> str:
    return f" on the SSH host (ssh_conn_id={ssh_conn_id!r})"


class SSHAnalyzeHook(DeferrableAnalyzeHook):
    """Runs `sparkforensics-analyze ... --format json [threshold flags]` on
    the host behind ssh_conn_id (the same Airflow connection SSHOperator,
    SFTPLogSourceHook and SSHTunneledLogSourceHook use) and reads the JSON
    report from its stdout. The event log never crosses the network, and the
    Airflow worker needs neither Node.js nor the CLI: that host needs
    Node.js 18+ and the sparkforensics-cli npm package. Reads a
    RemoteEventLog on the same ssh_conn_id (RemotePathLogSourceHook) or a
    HistoryServerApp (HistoryServerAppLogSourceHook), whose base_url is
    resolved on that host, e.g. http://localhost:18080. Requires the `ssh`
    extra.

    With SparkForensicsOperator(deferrable=True) the CLI instead runs as a
    detached job on that host (the SSH provider's remote-job wrapper, under
    remote_base_dir/<task instance>/), the task defers on the provider's
    SSHRemoteJobTrigger, polled every poll_interval seconds, and resumes to
    read the report file back. remote_base_dir defaults to
    $HOME/.sparkforensics/jobs of the SSH user."""

    supported_log_refs = (RemoteEventLog, HistoryServerApp)

    def __init__(
        self,
        ssh_conn_id: str,
        analyze_bin: str = "sparkforensics-analyze",
        timeout: int = 900,
        remote_base_dir: str | None = None,
        poll_interval: int = 5,
    ):
        super().__init__()
        if remote_base_dir is not None:
            _remote_job.validate_remote_base_dir(remote_base_dir)
        self.ssh_conn_id = ssh_conn_id
        self.analyze_bin = analyze_bin
        self.timeout = timeout
        self.remote_base_dir = remote_base_dir
        self.poll_interval = poll_interval
        # The synchronous run in progress in this process, for on_kill().
        self._sync_argv: list[str] | None = None
        self._sync_pid: int | None = None
        self._sync_channel = None

    @property
    def _where(self) -> str:
        return _where(self.ssh_conn_id)

    def check_log_ref(self, log_ref: EventLogRef) -> None:
        super().check_log_ref(log_ref)
        if isinstance(log_ref, RemoteEventLog) and log_ref.ssh_conn_id != self.ssh_conn_id:
            raise AirflowException(
                f"SSHAnalyzeHook(ssh_conn_id={self.ssh_conn_id!r}) cannot analyze an event log "
                f"on a different SSH host ({log_ref.describe()}). Use the same ssh_conn_id "
                "for the log source and the backend."
            )

    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        # Every argument is shlex-quoted for the remote POSIX shell, so a
        # rendered path, app id or analyze_bin can't inject shell syntax.
        # Closing a non-PTY channel doesn't signal the remote process, so
        # coreutils `timeout` bounds it on the host itself, and the command
        # reports its pid on the channel so on_kill() can stop it.
        argv = _remote_job.sync_cli_argv(self.analyze_bin, log_ref, thresholds, self.timeout)
        self._sync_argv = argv
        try:
            returncode, stdout, stderr = self._run_remote(_remote_job.sync_analysis_command(argv), log_ref)
        finally:
            self._sync_argv = None
            self._sync_pid = None
            self._sync_channel = None
        return _report_from_run(
            returncode, stdout, stderr, log_ref, thresholds,
            ssh_conn_id=self.ssh_conn_id, analyze_bin=self.analyze_bin, timeout=self.timeout,
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
                return _exec(
                    client, command, self.timeout,
                    on_timeout=lambda: _timeout_error(self.timeout, self.ssh_conn_id, log_ref),
                    on_channel=lambda channel: setattr(self, "_sync_channel", channel),
                    on_pid=lambda pid: setattr(self, "_sync_pid", pid),
                )
        except AirflowException:
            raise
        except Exception as e:
            raise AirflowException(
                f"SSH remote analysis failed (ssh_conn_id={self.ssh_conn_id!r}) while running "
                f"sparkforensics-analyze on {log_ref.describe()}: {e}"
            ) from e

    def on_kill(self) -> None:
        """Stops a running synchronous analysis when the task is killed:
        closes the channel, then signals the remote CLI's session, by the
        pid it reported, over a fresh connection."""
        argv, pid, channel = self._sync_argv, self._sync_pid, self._sync_channel
        if argv is None:
            return
        if channel is not None:
            try:
                channel.close()
            except Exception:
                self.log.debug("Closing the SSH channel failed", exc_info=True)
        if pid is None:
            return
        command = _remote_job.stop_sync_command(pid, argv)
        with self._job_connection("stopping the remote analysis") as client:
            self._checked(client, command, "stopping the remote analysis")

    # Deferrable mode (DeferrableAnalyzeHook). Every value collect() needs is
    # in the job dict, not on self: the operator that resumes is rebuilt
    # from the DAG file, and its backend is not the one that submitted.

    def submit(self, log_ref: EventLogRef, thresholds: dict, context: Any) -> dict:
        from airflow.providers.ssh.utils.remote_job import generate_job_id

        ti = context["ti"]
        scope = _remote_job.task_instance_scope(context)
        job_id = generate_job_id(
            dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, try_number=ti.try_number
        )
        with self._job_connection("submitting the remote analysis job") as client:
            # Stops and removes whatever an earlier try of this task instance
            # left running (a cleared or timed-out deferral) before starting
            # this one, so there is never more than one analysis per task
            # instance on the host.
            scope_dir = self._checked(
                client,
                _remote_job.prepare_scope_command(self.remote_base_dir, scope),
                "preparing the remote job directory",
            ).strip()
            try:
                # The default base directory comes from $HOME on the host,
                # which the constructor could not check.
                _remote_job.validate_remote_base_dir(scope_dir, "the remote job directory")
            except ValueError as e:
                raise AirflowException(
                    f"{e}{self._where}. Pass remote_base_dir=<a plain absolute path>."
                ) from e
            report_file = f"{scope_dir}/{job_id}/{_remote_job.REPORT_FILE_NAME}"
            stderr_file = f"{scope_dir}/{job_id}/{_remote_job.STDERR_FILE_NAME}"
            command = _remote_job.analysis_command(
                self.analyze_bin, log_ref, thresholds, self.timeout, report_file, stderr_file
            )
            submit, paths = _remote_job.submit_command(command, job_id, scope_dir)
            self._checked(client, submit, "submitting the remote analysis job")
        self.log.info(
            "Submitted sparkforensics-analyze%s as remote job %s in %s",
            self._where, job_id, paths["job_dir"],
        )
        return {
            "ssh_conn_id": self.ssh_conn_id,
            "analyze_bin": self.analyze_bin,
            "timeout": self.timeout,
            "poll_interval": self.poll_interval,
            "job_id": job_id,
            "scope_dir": scope_dir,
            "submitted_at": time.time(),
            **paths,
        }

    def trigger_for(self, job: dict) -> Any:
        from airflow.providers.ssh.triggers.ssh_remote_job import SSHRemoteJobTrigger

        return SSHRemoteJobTrigger(
            ssh_conn_id=job["ssh_conn_id"],
            remote_host=None,
            job_id=job["job_id"],
            job_dir=job["job_dir"],
            log_file=job["log_file"],
            exit_code_file=job["exit_code_file"],
            remote_os="posix",
            poll_interval=job["poll_interval"],
        )

    def defer_timeout(self, job: dict) -> timedelta:
        deadline = job["submitted_at"] + job["timeout"] + _DEFER_GRACE_S
        return timedelta(seconds=max(1.0, deadline - time.time()))

    def collect(self, job: dict, event: dict, log_ref: EventLogRef, thresholds: dict) -> Report:
        ssh_conn_id = job["ssh_conn_id"]
        if event.get("job_id") != job["job_id"]:
            raise AirflowException(
                f"Trigger event for remote job {event.get('job_id')!r} does not match the "
                f"submitted job {job['job_id']!r}{_where(ssh_conn_id)}."
            )
        exit_code = event.get("exit_code")
        if exit_code is None:
            # The trigger gave up (SSH unreachable past its reconnect
            # budget, or an unexpected error): the job may still be running.
            self._abandon_quietly(_remote_job.abandon_scope_dir_command(job["scope_dir"]), ssh_conn_id)
            raise AirflowException(
                f"SSH remote analysis failed (ssh_conn_id={ssh_conn_id!r}): lost track of remote "
                f"job {job['job_id']} while waiting for it: {event.get('message', 'unknown error')}"
            )

        with self._job_connection("reading the remote analysis result", ssh_conn_id) as client:
            try:
                stderr = self._read_file(client, job["stderr_file"]) or ""
                report_text = ""
                if exit_code in USABLE_REPORT_EXIT_CODES:
                    report_text = self._read_file(client, job["report_file"])
                    if report_text is None:
                        self.log.warning(
                            "Remote job %s exited %s but wrote no report file %s",
                            job["job_id"], exit_code, job["report_file"],
                        )
                        report_text = ""
            finally:
                # Read first, then clean up, on success and on failure alike.
                self._cleanup_job(client, job)
        return _report_from_run(
            exit_code, report_text, stderr, log_ref, thresholds,
            ssh_conn_id=ssh_conn_id, analyze_bin=job["analyze_bin"], timeout=job["timeout"],
        )

    def abandon(self, context: Any) -> None:
        command = _remote_job.abandon_scope_command(
            self.remote_base_dir, _remote_job.task_instance_scope(context)
        )
        with self._job_connection("stopping the remote analysis job") as client:
            self._checked(client, command, "stopping the remote analysis job")

    def _job_connection(self, what: str, ssh_conn_id: str | None = None):
        return _JobConnection(ssh_conn_id or self.ssh_conn_id, what)

    def _checked(self, client, command: str, what: str) -> str:
        returncode, stdout, stderr = _exec(client, command, _JOB_COMMAND_TIMEOUT_S)
        if returncode != 0:
            raise AirflowException(
                f"SSH remote analysis failed{self._where} while {what} (exit {returncode}): "
                f"{stderr.strip()}"
            )
        return stdout

    def _read_file(self, client, path: str) -> str | None:
        returncode, stdout, _ = _exec(client, _remote_job.read_file_command(path), _JOB_COMMAND_TIMEOUT_S)
        return stdout if returncode == 0 else None

    def _cleanup_job(self, client, job: dict) -> None:
        try:
            command = _remote_job.cleanup_job_command(job["job_dir"], job["scope_dir"])
            returncode, _, stderr = _exec(client, command, _JOB_COMMAND_TIMEOUT_S)
            if returncode != 0:
                raise AirflowException(f"exit {returncode}: {stderr.strip()}")
        except Exception as e:
            self.log.warning(
                "Could not remove remote job directory %s%s; the next try of this task "
                "removes it: %s", job["job_dir"], _where(job["ssh_conn_id"]), e,
            )

    def _abandon_quietly(self, command: str, ssh_conn_id: str) -> None:
        try:
            with self._job_connection("stopping the remote analysis job", ssh_conn_id) as client:
                self._checked(client, command, "stopping the remote analysis job")
        except Exception as e:
            self.log.warning(
                "Could not stop and remove the remote job%s; the next try of this task "
                "does: %s", _where(ssh_conn_id), e,
            )


class _JobConnection:
    """An SSHHook client for one step of a deferrable run, with any
    connection-level failure raised as the same AirflowException the
    synchronous path raises."""

    def __init__(self, ssh_conn_id: str, what: str):
        self.ssh_conn_id = ssh_conn_id
        self.what = what
        self._conn = None

    def __enter__(self):
        from airflow.providers.ssh.hooks.ssh import SSHHook

        try:
            self._conn = SSHHook(ssh_conn_id=self.ssh_conn_id).get_conn()
            return self._conn.__enter__()
        except Exception as e:
            raise self._error(e) from e

    def __exit__(self, exc_type, exc, tb):
        self._conn.__exit__(exc_type, exc, tb)
        if exc is None or isinstance(exc, AirflowException):
            return False
        raise self._error(exc) from exc

    def _error(self, e: BaseException) -> AirflowException:
        return AirflowException(
            f"SSH remote analysis failed (ssh_conn_id={self.ssh_conn_id!r}) while {self.what}: {e}"
        )


def _exec(
    client, command: str, timeout: float, on_timeout=None, on_channel=None, on_pid=None
) -> tuple[int, str, str]:
    """Runs command on an open paramiko client and returns (exit status,
    stdout, stderr), bounding the whole run by timeout seconds. on_channel,
    if given, receives the channel as soon as it is open; on_pid, the pid
    sync_analysis_command's launcher reports, as soon as it arrives, and
    that line is left out of stdout."""
    stdin, stdout, _ = client.exec_command(command, timeout=timeout)
    channel = stdout.channel
    if on_channel is not None:
        on_channel(channel)
    stdin.close()
    channel.shutdown_write()

    out_chunks: list[bytes] = []
    err_chunks: list[bytes] = []
    pid_pending = on_pid is not None
    deadline = time.monotonic() + timeout
    while True:
        received = False
        while channel.recv_ready():
            out_chunks.append(channel.recv(_READ_CHUNK_BYTES))
            received = True
        if pid_pending and received:
            pid_pending = not _take_pid_line(out_chunks, on_pid)
        while channel.recv_stderr_ready():
            err_chunks.append(channel.recv_stderr(_READ_CHUNK_BYTES))
            received = True
        if channel.exit_status_ready() and not (channel.recv_ready() or channel.recv_stderr_ready()):
            break
        if time.monotonic() > deadline:
            channel.close()
            if on_timeout is not None:
                raise on_timeout()
            raise TimeoutError(f"remote command did not finish within {timeout}s")
        if not received:
            time.sleep(_POLL_INTERVAL_S)
    return (
        channel.recv_exit_status(),
        b"".join(out_chunks).decode("utf-8", "replace"),
        b"".join(err_chunks).decode("utf-8", "replace"),
    )


def _take_pid_line(out_chunks: list[bytes], on_pid) -> bool:
    """Finds the launcher's pid line in the stdout read so far (a login
    shell may print something before it); if there, removes it and hands
    the pid to on_pid."""
    buffered = b"".join(out_chunks)
    match = _PID_LINE.search(buffered)
    if match is None:
        return False
    out_chunks[:] = [buffered[:match.start()] + buffered[match.end():]]
    on_pid(int(match.group(1)))
    return True


def _timeout_error(timeout: int, ssh_conn_id: str, log_ref: EventLogRef) -> AirflowException:
    return AirflowException(
        f"sparkforensics-analyze timed out after {timeout}s"
        f"{_where(ssh_conn_id)} analyzing {log_ref.describe()}."
    )


def _report_from_run(
    returncode: int,
    report_text: str,
    stderr: str,
    log_ref: EventLogRef,
    thresholds: dict,
    *,
    ssh_conn_id: str,
    analyze_bin: str,
    timeout: int,
) -> Report:
    """The one mapping from a remote CLI run's outcome to a Report or an
    error, shared by the synchronous and the deferrable path so both raise
    the same error for the same outcome."""
    where = _where(ssh_conn_id)
    if returncode == _REMOTE_TIMEOUT_EXIT_CODE:
        raise _timeout_error(timeout, ssh_conn_id, log_ref)
    if returncode in _SHELL_COMMAND_NOT_FOUND_EXIT_CODES:
        raise AirflowException(
            f"sparkforensics-analyze binary not found or not executable{where}: "
            f"{analyze_bin!r} or the coreutils `timeout` it runs under (exit {returncode}). "
            "Check the stderr below for which one. Install the CLI on that host with "
            "`npm install -g sparkforensics-cli` (Node.js 18+), or pass "
            "analyze_bin=<full path to sparkforensics-analyze>: a non-interactive SSH "
            "session may not load the PATH a login shell sets (e.g. for nvm). "
            f"{stderr.strip()}"
        )
    check_exit_code(returncode, stderr, where)
    return build_report(report_text, thresholds, stderr, returncode, where)
