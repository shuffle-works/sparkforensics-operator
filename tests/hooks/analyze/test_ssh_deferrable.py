"""SSHAnalyzeHook's deferrable mode against a real local shell standing in
for the SSH host (see _local_ssh.py): submit() really starts the provider's
detached job, SSHRemoteJobTrigger really polls it, collect() really reads
the files back."""
import importlib
import json
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog

from ._local_ssh import (
    SAMPLE_JSON,
    LocalHost,
    needs_posix_host,
    pid_alive,
    run_trigger,
    ti_context,
    wait_until,
)

pytest.importorskip("airflow.providers.ssh.triggers.ssh_remote_job")

pytestmark = needs_posix_host

CONN = "onprem_ssh"
LOG = RemoteEventLog(CONN, "/logs/app-1")


@pytest.fixture
def host(tmp_path):
    host = LocalHost(tmp_path)
    with host.patched():
        yield host


def _hook(**kwargs):
    kwargs.setdefault("poll_interval", 0.1)
    return SSHAnalyzeHook(ssh_conn_id=CONN, **kwargs)


def _run_deferred(hook, log_ref=LOG, thresholds=None, context=None):
    thresholds = thresholds or {}
    context = context or ti_context()
    job = hook.submit(log_ref, thresholds, context)
    event = run_trigger(hook.trigger_for(job))
    return job, event, lambda: hook.collect(job, event, log_ref, thresholds)


def _scope_dirs(host):
    base = host.home / ".sparkforensics" / "jobs"
    return sorted(p.name for p in base.iterdir()) if base.exists() else []


def test_deferred_run_reads_the_report_file_and_the_stderr_threshold_lines(host):
    host.fake_cli(f"""\
        echo 'progress on stdout'
        printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit
        echo '[violation] max-spill: 3.1 GB > 2 GB' >&2
        exit 1
    """)
    job, event, collect = _run_deferred(_hook(), thresholds={"max_spill_gb": 2})
    streamed_log = Path(job["log_file"]).read_text()

    report = collect()

    assert event["exit_code"] == 1
    assert report.findings == SAMPLE_JSON["findings"]
    assert report.exit_code == 1
    assert [(r.name, r.status, r.detail) for r in report.threshold_results] == [
        ("max-spill", "violation", "3.1 GB > 2 GB")
    ]
    # The report came from --out inside the job directory, not the log.
    assert host.argv()[host.argv().index("--out") + 1] == job["report_file"]
    # The log the trigger streams to the task log has the CLI's stdout, but
    # neither the report nor stderr.
    assert "progress on stdout" in streamed_log
    assert "schemaVersion" not in streamed_log
    assert "violation" not in streamed_log


def test_deferred_run_removes_the_job_directory_after_reading_it(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    job, _, collect = _run_deferred(_hook())
    assert Path(job["report_file"]).exists()

    collect()

    assert not Path(job["job_dir"]).exists()
    assert not Path(job["scope_dir"]).exists()


@pytest.mark.parametrize("exit_code", [2, 5])
def test_deferred_run_removes_the_job_directory_on_failure_too(host, exit_code):
    host.fake_cli(f"echo 'bad log' >&2; exit {exit_code}\n")
    job, _, collect = _run_deferred(_hook())

    with pytest.raises(AirflowException):
        collect()

    assert not Path(job["scope_dir"]).exists()


def test_job_directory_is_owner_only_under_the_ssh_users_home(host):
    host.fake_cli("sleep 5\n")
    hook = _hook()
    job = hook.submit(LOG, {}, ti_context())
    try:
        scope = Path(job["scope_dir"])
        assert scope.parent == host.home / ".sparkforensics" / "jobs"
        assert scope.stat().st_mode & 0o777 == 0o700
    finally:
        hook.abandon(ti_context())


def test_remote_timeout_kills_the_cli_and_its_children_and_raises_the_timeout_error(host):
    host.fake_cli(f"""\
        echo $$ > {host.home}/cli_pid
        sleep 30 &
        echo $! > {host.home}/child_pid
        wait
    """)
    job, event, collect = _run_deferred(_hook(timeout=1))

    assert event["exit_code"] == 124
    with pytest.raises(AirflowException, match=r"timed out after 1s on the SSH host \(ssh_conn_id='onprem_ssh'\)"):
        collect()
    for name in ("cli_pid", "child_pid"):
        pid = int((host.home / name).read_text())
        assert wait_until(lambda: not pid_alive(pid)), f"{name} {pid} still running"
    assert not Path(job["scope_dir"]).exists()


def test_a_new_try_stops_and_removes_the_previous_try_before_starting(host):
    host.fake_cli(f"echo $$ > {host.home}/cli_pid_$$; sleep 30\n")
    hook = _hook()
    first = hook.submit(LOG, {}, ti_context(try_number=1))
    assert wait_until(lambda: list(host.home.glob("cli_pid_*")))
    first_pid = int(next(host.home.glob("cli_pid_*")).read_text())

    second = hook.submit(LOG, {}, ti_context(try_number=2))
    try:
        assert wait_until(lambda: not pid_alive(first_pid))
        assert not Path(first["job_dir"]).exists()
        assert Path(second["job_dir"]).exists()
        assert first["scope_dir"] == second["scope_dir"]
    finally:
        hook.abandon(ti_context(try_number=2))


def test_other_task_instances_are_left_alone(host):
    host.fake_cli("sleep 30\n")
    hook = _hook()
    mapped_0 = hook.submit(LOG, {}, ti_context(map_index=0))
    mapped_1 = hook.submit(LOG, {}, ti_context(map_index=1))
    try:
        assert mapped_0["scope_dir"] != mapped_1["scope_dir"]
        hook.abandon(ti_context(map_index=0))
        assert not Path(mapped_0["scope_dir"]).exists()
        assert Path(mapped_1["job_dir"]).exists()
    finally:
        hook.abandon(ti_context(map_index=1))


def test_abandon_kills_the_running_job_and_removes_its_directory(host):
    host.fake_cli(f"echo $$ > {host.home}/cli_pid; sleep 30\n")
    hook = _hook()
    job = hook.submit(LOG, {}, ti_context())
    assert wait_until(lambda: (host.home / "cli_pid").exists())
    pid = int((host.home / "cli_pid").read_text())

    hook.abandon(ti_context())

    assert wait_until(lambda: not pid_alive(pid))
    assert not Path(job["scope_dir"]).exists()
    assert _scope_dirs(host) == []


def test_sweep_never_signals_a_pid_that_is_no_longer_its_job(host, tmp_path):
    # Simulates a host reboot: the recorded pid now belongs to an unrelated
    # process, which must survive the sweep.
    import subprocess

    bystander = subprocess.Popen(["sleep", "30"])
    try:
        hook = _hook()
        host.fake_cli("sleep 30\n")
        job = hook.submit(LOG, {}, ti_context())
        hook.abandon(ti_context())
        Path(job["job_dir"]).mkdir(parents=True)
        Path(job["job_dir"], "pid").write_text(str(bystander.pid))

        hook.abandon(ti_context())

        assert bystander.poll() is None
        assert not Path(job["scope_dir"]).exists()
    finally:
        bystander.kill()


def test_every_remote_argument_and_path_is_quoted(host, tmp_path):
    nasty = "/logs/it's a \"log\" $(touch pwned) `touch pwned2`; touch pwned3 &.json"
    base = str(tmp_path / "base dir's (x) & y")
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook(remote_base_dir=base)
    ref = HistoryServerApp("http://localhost:18080/$(id)", "app-'1'", "2;x")

    _, _, collect = _run_deferred(hook, log_ref=RemoteEventLog(CONN, nasty))
    collect()
    assert host.argv()[0] == nasty

    _, _, collect = _run_deferred(hook, log_ref=ref)
    collect()
    argv = host.argv()
    assert argv[:6] == ["--shs-base-url", "http://localhost:18080/$(id)", "--app-id", "app-'1'", "--attempt-id", "2;x"]
    assert not list(host.home.rglob("pwned*"))


def test_missing_binary_raises_the_same_error_as_the_synchronous_path(host):
    hook = _hook(analyze_bin="/nonexistent/sparkforensics-analyze")
    _, event, collect = _run_deferred(hook)

    with pytest.raises(AirflowException) as deferred:
        collect()
    with pytest.raises(AirflowException) as sync:
        hook.analyze(LOG, {})

    assert event["exit_code"] == 127
    assert "binary not found or not executable" in str(deferred.value)
    assert str(deferred.value) == str(sync.value)


@pytest.mark.parametrize(
    "body",
    [
        "echo 'cannot parse log' >&2; exit 2",
        "echo 'oom' >&2; exit 137",
        "printf '%s' '{\"schemaVersion\": 3, \"summ' | emit",
        "exit 0",
    ],
    ids=["exit-2", "unexpected-exit", "truncated-report", "no-report"],
)
def test_failures_raise_the_same_error_as_the_synchronous_path(host, body):
    host.fake_cli(body + "\n")
    hook = _hook()
    _, _, collect = _run_deferred(hook)

    with pytest.raises(AirflowException) as deferred:
        collect()
    with pytest.raises(AirflowException) as sync:
        hook.analyze(LOG, {})

    assert str(deferred.value) == str(sync.value)


def test_ssh_failure_at_submit_raises_a_clear_airflowexception(host):
    host.fail_connect = OSError("Connection refused")

    with pytest.raises(AirflowException, match=r"SSH remote analysis failed \(ssh_conn_id='onprem_ssh'\) while submitting.*Connection refused"):
        _hook().submit(LOG, {}, ti_context())


def test_ssh_failure_at_resume_raises_a_clear_airflowexception(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook()
    job, event, collect = _run_deferred(hook)
    host.fail_connect = OSError("No route to host")

    with pytest.raises(AirflowException, match=r"SSH remote analysis failed \(ssh_conn_id='onprem_ssh'\) while reading.*No route to host"):
        collect()

    # The next try's sweep removes what could not be cleaned up.
    host.fail_connect = None
    hook.abandon(ti_context())
    assert not Path(job["scope_dir"]).exists()


def test_a_trigger_error_stops_the_job_and_raises(host):
    host.fake_cli(f"echo $$ > {host.home}/cli_pid; sleep 30\n")
    hook = _hook()
    job = hook.submit(LOG, {}, ti_context())
    assert wait_until(lambda: (host.home / "cli_pid").exists())
    pid = int((host.home / "cli_pid").read_text())
    event = {"job_id": job["job_id"], "done": True, "exit_code": None, "message": "Trigger error: boom"}

    with pytest.raises(AirflowException, match="lost track of remote job .*Trigger error: boom"):
        hook.collect(job, event, LOG, {})

    assert wait_until(lambda: not pid_alive(pid))
    assert not Path(job["scope_dir"]).exists()


def test_collect_rejects_an_event_for_another_job(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook()
    job, event, _ = _run_deferred(hook)

    with pytest.raises(AirflowException, match="does not match"):
        hook.collect(job, {**event, "job_id": "af_other"}, LOG, {})
    hook.abandon(ti_context())


def test_collect_uses_the_submitted_configuration_not_the_resumed_hooks(host):
    host.fake_cli("exit 127\n")
    job, event, _ = _run_deferred(_hook(analyze_bin="sparkforensics-analyze"))

    resumed = SSHAnalyzeHook(ssh_conn_id="edited_conn", analyze_bin="/edited/bin")
    with pytest.raises(AirflowException, match=r"\(ssh_conn_id='onprem_ssh'\): 'sparkforensics-analyze' or"):
        resumed.collect(job, event, LOG, {})


def test_trigger_and_job_survive_serialization(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook()
    job = hook.submit(LOG, {}, ti_context())
    classpath, kwargs = hook.trigger_for(job).serialize()

    module, _, name = classpath.rpartition(".")
    rebuilt = getattr(importlib.import_module(module), name)(**json.loads(json.dumps(kwargs)))
    event = run_trigger(rebuilt)
    report = hook.collect(json.loads(json.dumps(job)), event, LOG, {})

    assert report.findings == SAMPLE_JSON["findings"]


def test_defer_timeout_is_a_backstop_past_the_remote_timeout(host):
    host.fake_cli("sleep 5\n")
    hook = _hook(timeout=600)
    job = hook.submit(LOG, {}, ti_context())
    try:
        seconds = hook.defer_timeout(job).total_seconds()
        assert 600 < seconds <= 720
        assert hook.defer_timeout({**job, "submitted_at": 0}).total_seconds() == 1
    finally:
        hook.abandon(ti_context())


def test_submit_rejects_a_log_the_ssh_host_cannot_read(host, tmp_path):
    with pytest.raises(AirflowException, match="cannot analyze"):
        _hook().check_log_ref(LocalEventLog(tmp_path / "app.log"))
    with pytest.raises(AirflowException, match="different SSH host"):
        _hook().check_log_ref(RemoteEventLog("other", "/logs/app-1"))


@pytest.mark.parametrize("base", ["relative/dir", "/a/../etc", "/a\nb", "/a/$(id)", "/a/`id`", '/a/"b', "/a\\b"])
def test_remote_base_dir_must_be_a_plain_absolute_path(base):
    with pytest.raises(ValueError, match="remote_base_dir"):
        SSHAnalyzeHook(ssh_conn_id=CONN, remote_base_dir=base)


def test_submit_rejects_a_home_directory_the_wrapper_cannot_quote(host, tmp_path):
    weird_home = tmp_path / "home$(id)"
    weird_home.mkdir()
    host.env["HOME"] = str(weird_home)

    with pytest.raises(AirflowException, match=r"the remote job directory cannot contain \$.*Pass remote_base_dir"):
        _hook().submit(LOG, {}, ti_context())


def test_submit_fails_clearly_when_the_job_directory_cannot_be_created(host):
    blocker = host.home / "not-a-dir"
    blocker.write_text("")

    with pytest.raises(AirflowException, match=r"while preparing the remote job directory \(exit \d+\)"):
        _hook(remote_base_dir=str(blocker / "jobs")).submit(LOG, {}, ti_context())


def test_collect_never_removes_a_directory_outside_the_task_instance_scope(host, tmp_path):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook()
    job, event, _ = _run_deferred(hook)
    outside = tmp_path / "outside"
    outside.mkdir()

    report = hook.collect({**job, "job_dir": str(outside)}, event, LOG, {})

    assert report.findings == SAMPLE_JSON["findings"]
    assert outside.exists()
    hook.abandon(ti_context())


def test_a_trigger_error_still_raises_when_the_job_cannot_be_stopped(host):
    host.fake_cli("sleep 5\n")
    hook = _hook()
    job = hook.submit(LOG, {}, ti_context())
    host.fail_connect = OSError("No route to host")
    event = {"job_id": job["job_id"], "done": True, "exit_code": None, "message": "Trigger error: boom"}

    with pytest.raises(AirflowException, match="lost track of remote job"):
        hook.collect(job, event, LOG, {})

    host.fail_connect = None
    hook.abandon(ti_context())


def test_a_connection_dropping_while_reading_the_result_raises_a_clear_airflowexception(host):
    host.fake_cli(f"printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit\n")
    hook = _hook()
    job, event, collect = _run_deferred(hook)
    host.fail_exec_matching = "cat --"

    with pytest.raises(AirflowException, match=r"while reading the remote analysis result: connection dropped"):
        collect()

    # Cleanup still ran on the same connection.
    assert not Path(job["scope_dir"]).exists()


def test_a_job_command_that_never_finishes_times_out():
    from sparkforensics_operator.hooks.analyze.ssh import _exec

    from .test_ssh import _FakeChannel, _ssh_hook_for

    channel = _FakeChannel(exit_status=None)
    _, client = _ssh_hook_for(channel)

    with pytest.raises(TimeoutError, match="did not finish within 0s"):
        _exec(client, "sleep 99", 0)
    assert channel.closed
