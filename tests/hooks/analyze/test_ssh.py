import json
import threading
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog

from ._local_ssh import LocalHost, needs_posix_host, pid_alive, wait_until

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


@pytest.fixture
def host(tmp_path):
    host = LocalHost(tmp_path)
    with host.patched():
        yield host


def _jobs_left(host):
    base = host.home / ".sparkforensics" / "jobs"
    return list(base.iterdir()) if base.exists() else []


@needs_posix_host
def test_analyze_runs_the_cli_on_the_ssh_host_against_a_remote_path_and_reads_stdout(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=60)

    report = hook.analyze(RemoteEventLog(ssh_conn_id="onprem_ssh", path="/logs/app-1"), {"max_runtime_ms": 10_000})

    assert host.argv() == ["/logs/app-1", "--format", "json", "--max-runtime", "10000"]
    assert report.schema_version == 3
    assert report.exit_code == 0
    assert report.threshold_results[0].status == "pass"
    assert _jobs_left(host) == []


@needs_posix_host
def test_analyze_passes_a_history_server_app_to_the_remote_cli(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")

    hook.analyze(HistoryServerApp(base_url="http://localhost:18080", app_id="app-1", attempt_id="1"), {})

    assert host.argv() == [
        "--shs-base-url", "http://localhost:18080", "--app-id", "app-1", "--attempt-id", "1", "--format", "json",
    ]


@needs_posix_host
def test_analyze_quotes_every_argument_for_the_remote_shell(host):
    cli = host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n", name="spark forensics's analyze")
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", analyze_bin=str(cli))
    hostile_path = "/logs/app-1; touch pwned $(touch pwned2) `touch pwned3` \"x\" 'y' &"

    hook.analyze(RemoteEventLog(ssh_conn_id="onprem_ssh", path=hostile_path), {})

    assert host.argv()[0] == hostile_path
    assert not list(host.home.rglob("pwned*"))


@needs_posix_host
def test_the_remote_timeout_stops_a_synchronous_cli_and_its_children(host):
    host.fake_cli(f"""\
        echo $$ > {host.home}/cli_pid
        sleep 30 &
        echo $! > {host.home}/child_pid
        wait
    """)
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=1)

    with pytest.raises(AirflowException, match="timed out after 1s on the SSH host"):
        hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})

    for name in ("cli_pid", "child_pid"):
        pid = int((host.home / name).read_text())
        assert wait_until(lambda: not pid_alive(pid)), f"{name} {pid} still running"
    assert _jobs_left(host) == []


@needs_posix_host
def test_on_kill_stops_a_running_synchronous_analysis_on_the_host(host):
    host.fake_cli(f"""\
        echo $$ > {host.home}/cli_pid
        sleep 30 &
        echo $! > {host.home}/child_pid
        wait
    """)
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=60)
    outcome = {}

    def _analyze():
        try:
            hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})
        except AirflowException as e:
            outcome["error"] = e

    worker = threading.Thread(target=_analyze)
    worker.start()
    assert wait_until(lambda: (host.home / "child_pid").exists() and hook._sync_pid is not None)

    hook.on_kill()
    worker.join(timeout=20)

    assert not worker.is_alive()
    assert "error" in outcome
    for name in ("cli_pid", "child_pid"):
        pid = int((host.home / name).read_text())
        assert wait_until(lambda: not pid_alive(pid)), f"{name} {pid} still running"
    assert _jobs_left(host) == []


def test_on_kill_does_nothing_when_no_analysis_is_running():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook") as ssh_cls:
        hook.on_kill()

    ssh_cls.assert_not_called()


@needs_posix_host
def test_a_synchronous_analysis_writes_nothing_on_the_host(host):
    blocker = host.home / "not-a-dir"
    blocker.write_text("")
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", remote_base_dir=str(blocker / "jobs"))
    before = sorted(host.home.rglob("*"))

    report = hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})

    assert report.schema_version == 3
    assert report.summary == SAMPLE_JSON["summary"]
    assert sorted(p for p in host.home.rglob("*") if p.name != "argv") == before


def test_analyze_leaves_the_pid_line_out_of_the_report():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    channel = _FakeChannel(
        stdout=b"sparkforensics-pid:4242\n" + json.dumps(SAMPLE_JSON).encode(), exit_status=0, chunk=5
    )

    report, _, _ = _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)

    assert report.schema_version == 3
    assert report.summary == SAMPLE_JSON["summary"]


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
    assert "coreutils `timeout`" in message
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


def test_analyze_raises_the_timeout_error_when_the_remote_timeout_stops_the_cli():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=5)
    channel = _FakeChannel(exit_status=124)

    with pytest.raises(AirflowException, match="timed out after 5s on the SSH host") as exc_info:
        _run(hook, RemoteEventLog("onprem_ssh", "/logs/app-1"), {}, channel)

    assert "/logs/app-1" in str(exc_info.value)


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


class _KillableChannel(_FakeChannel):
    """A channel that runs until on_kill() closes it; close() itself fails,
    as closing a dropped connection can."""

    def __init__(self):
        super().__init__(stdout=b"sparkforensics-pid:4242\n", exit_status=None)

    def exit_status_ready(self):
        return self.closed

    def recv_exit_status(self):
        return 143

    def close(self):
        self.closed = True
        raise EOFError("connection already gone")


def test_on_kill_closes_the_live_channel_and_stops_the_reported_pid():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    running = _KillableChannel()
    stop = _FakeChannel(exit_status=0)
    ssh_hook, client = _ssh_hook_for(running)
    client.exec_command.side_effect = [
        (MagicMock(), MagicMock(channel=running), MagicMock()),
        (MagicMock(), MagicMock(channel=stop), MagicMock()),
    ]
    outcome = {}

    def _analyze():
        try:
            hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})
        except AirflowException as e:
            outcome["error"] = e

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook):
        worker = threading.Thread(target=_analyze)
        worker.start()
        assert wait_until(lambda: hook._sync_pid == 4242)
        hook.on_kill()
        worker.join(timeout=10)

    assert running.closed
    assert "unexpected code 143" in str(outcome["error"])
    # The run's command, then the one that stops it on the host (what that
    # does there is covered against a real shell above).
    assert client.exec_command.call_count == 2


def test_on_kill_before_the_pid_arrives_only_closes_the_channel():
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    running = _KillableChannel()
    running._stdout = []
    ssh_hook, client = _ssh_hook_for(running)
    outcome = {}

    def _analyze():
        try:
            hook.analyze(RemoteEventLog("onprem_ssh", "/logs/app-1"), {})
        except AirflowException as e:
            outcome["error"] = e

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook):
        worker = threading.Thread(target=_analyze)
        worker.start()
        assert wait_until(lambda: hook._sync_channel is running)
        hook.on_kill()
        worker.join(timeout=10)

    assert running.closed
    assert client.exec_command.call_count == 1


def test_deferral_needs_the_ssh_provider_the_remote_job_helpers_come_from(monkeypatch):
    import airflow.providers.ssh as provider

    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh")
    monkeypatch.setattr(provider, "__version__", "6.0.1")
    assert hook.cannot_defer_reason() is None
    monkeypatch.setattr(provider, "__version__", "3.7.1")
    assert hook.cannot_defer_reason() == (
        "it needs apache-airflow-providers-ssh>=6.0.1 (Airflow 2.11+), and 3.7.1 is installed"
    )


def test_deferrable_true_on_an_older_ssh_provider_fails_at_dag_parse(monkeypatch):
    import airflow.providers.ssh as provider

    from sparkforensics_operator.operator import SparkForensicsOperator

    monkeypatch.setattr(provider, "__version__", "3.7.1")
    with pytest.raises(ValueError, match=r"cannot run SSHAnalyzeHook detached here: it needs apache-airflow-providers-ssh>=6\.0\.1"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=SSHAnalyzeHook(ssh_conn_id="onprem_ssh"),
            report_dest="/tmp/r.json", deferrable=True,
        )
