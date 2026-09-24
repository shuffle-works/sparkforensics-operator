import json
import shlex
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog

pytest.importorskip("airflow.providers.ssh.hooks.ssh")

SAMPLE_JSON = {
    "schemaVersion": 3,
    "summary": {"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
    "findings": [],
    "recommendations": [],
    "cleanChecks": [],
}


class _FakeChannel:
    """Stands in for a paramiko Channel: hands out stdout/stderr in chunks,
    then reports the exit status (or never finishes, when exit_status is
    None)."""

    def __init__(self, stdout=b"", stderr=b"", exit_status=0, chunk=16):
        self._stdout = [stdout[i:i + chunk] for i in range(0, len(stdout), chunk)]
        self._stderr = [stderr[i:i + chunk] for i in range(0, len(stderr), chunk)]
        self._exit_status = exit_status
        self.closed = False

    def recv_ready(self):
        return bool(self._stdout)

    def recv(self, _nbytes):
        return self._stdout.pop(0)

    def recv_stderr_ready(self):
        return bool(self._stderr)

    def recv_stderr(self, _nbytes):
        return self._stderr.pop(0)

    def exit_status_ready(self):
        return self._exit_status is not None

    def recv_exit_status(self):
        return self._exit_status

    def shutdown_write(self):
        pass

    def close(self):
        self.closed = True


def _ssh_hook_for(channel):
    """An SSHHook mock whose get_conn() client runs every command on channel,
    recording the command it was given."""
    client = MagicMock()
    client.__enter__.return_value = client
    stdout = MagicMock()
    stdout.channel = channel
    client.exec_command.return_value = (MagicMock(), stdout, MagicMock())
    ssh_hook = MagicMock()
    ssh_hook.get_conn.return_value = client
    return ssh_hook, client


def _run(hook, log_ref, thresholds, channel):
    ssh_hook, client = _ssh_hook_for(channel)
    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook) as ssh_cls:
        report = hook.analyze(log_ref, thresholds)
    ssh_cls.assert_called_once_with(ssh_conn_id=hook.ssh_conn_id)
    command = client.exec_command.call_args.args[0]
    return report, command, client


def test_analyze_runs_the_cli_on_the_ssh_host_against_a_remote_path_and_reads_stdout():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=60)
    channel = _FakeChannel(stdout=json.dumps(SAMPLE_JSON).encode())

    report, command, client = _run(
        hook, RemoteEventLog(ssh_conn_id="onprem_ssh", path="/logs/app-1"), {"max_runtime_ms": 10_000}, channel,
    )

    assert shlex.split(command) == [
        "sparkforensics-analyze", "/logs/app-1", "--format", "json", "--max-runtime", "10000",
    ]
    assert client.exec_command.call_args.kwargs == {"timeout": 60}
    assert report.schema_version == 3
    assert report.exit_code == 0
    assert report.threshold_results[0].status == "pass"


def test_analyze_passes_a_history_server_app_to_the_remote_cli():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(stdout=json.dumps(SAMPLE_JSON).encode())

    _, command, _ = _run(
        hook, HistoryServerApp(base_url="http://localhost:18080", app_id="app-1", attempt_id="1"), {}, channel,
    )

    assert shlex.split(command) == [
        "sparkforensics-analyze", "--shs-base-url", "http://localhost:18080", "--app-id", "app-1",
        "--attempt-id", "1", "--format", "json",
    ]


def test_analyze_quotes_every_argument_for_the_remote_shell():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", analyze_bin="/opt/node bin/sparkforensics-analyze")
    channel = _FakeChannel(stdout=json.dumps(SAMPLE_JSON).encode())
    hostile_path = "/logs/app-1; rm -rf ~ $(whoami) `id`"

    _, command, _ = _run(hook, RemoteEventLog(ssh_conn_id="onprem_ssh", path=hostile_path), {}, channel)

    assert shlex.split(command)[:2] == ["/opt/node bin/sparkforensics-analyze", hostile_path]
    assert f"'{hostile_path}'" in command


def test_analyze_parses_threshold_violations_from_remote_stderr():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(
        stdout=json.dumps(SAMPLE_JSON).encode(),
        stderr=b"[violation] max-runtime: Runtime 12000ms exceeds budget 10000ms.\n",
        exit_status=1,
    )

    report, _, _ = _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {"max_runtime_ms": 10_000}, channel)

    assert report.violated
    assert report.exit_code == 1
    assert report.threshold_results[0].detail == "Runtime 12000ms exceeds budget 10000ms."


@pytest.mark.parametrize("exit_status", [126, 127])
def test_analyze_raises_a_clear_error_when_the_binary_is_missing_on_the_ssh_host(exit_status):
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(stderr=b"sh: 1: sparkforensics-analyze: not found\n", exit_status=exit_status)

    with pytest.raises(AirflowException, match="binary not found or not executable on the SSH host") as exc_info:
        _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)

    message = str(exc_info.value)
    assert "ssh_conn_id='onprem_ssh'" in message
    assert "npm install -g sparkforensics-cli" in message
    assert "not found" in message


def test_analyze_raises_on_exit_code_2_naming_the_ssh_host():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(stderr=b"History Server returned 404\n", exit_status=2)

    with pytest.raises(AirflowException, match=r"on the SSH host .* \(exit 2\): History Server returned 404"):
        _run(hook, HistoryServerApp(base_url="http://localhost:18080", app_id="app-1"), {}, channel)


def test_analyze_raises_on_an_unexpected_exit_code():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(stderr=b"Killed\n", exit_status=137)

    with pytest.raises(AirflowException, match="unexpected code 137: Killed"):
        _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)


def test_analyze_raises_a_clear_error_when_stdout_is_not_a_json_report():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(stdout=b"Welcome to the bastion!\n{", exit_status=0)

    with pytest.raises(AirflowException, match="JSON report could not be parsed"):
        _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)


def test_analyze_times_out_on_wall_clock_and_closes_the_channel():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=5)
    channel = _FakeChannel(exit_status=None)  # never finishes

    with patch("sparkforensics_operator.hooks.analyze.ssh.time") as fake_time:
        fake_time.monotonic.side_effect = [0, 1, 6]
        with pytest.raises(AirflowException, match="timed out after 5s on the SSH host") as exc_info:
            _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)

    assert "/logs/app-1" in str(exc_info.value)
    assert channel.closed
    fake_time.sleep.assert_called_once()


def test_analyze_wraps_an_ssh_failure_in_an_airflowexception():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    ssh_hook = MagicMock()
    ssh_hook.get_conn.side_effect = OSError("no route to host")

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook):
        with pytest.raises(AirflowException, match="SSH remote analysis failed.*onprem_ssh.*no route to host"):
            hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})


def test_analyze_rejects_a_remote_log_on_a_different_ssh_connection():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook") as ssh_cls:
        with pytest.raises(AirflowException, match="different SSH host"):
            hook.analyze(RemoteEventLog("other_ssh", "/logs/app-1"), {})

    ssh_cls.assert_not_called()


def test_analyze_rejects_a_log_fetched_to_the_worker(tmp_path):
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook") as ssh_cls:
        with pytest.raises(AirflowException, match="SSHAnalyzeHook cannot analyze LocalEventLog"):
            hook.analyze(LocalEventLog(tmp_path / "app.log"), {})

    ssh_cls.assert_not_called()
