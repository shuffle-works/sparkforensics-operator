"""The same SparkForensicsOperator input, run synchronously and deferred
through SSHAnalyzeHook against the local stand-in SSH host (see
_local_ssh.py), must give identical results: persisted report, threshold
results, return value, ReportLink and notifier calls. The deferred run goes
through everything Airflow does in between: serializing the trigger and the
resume kwargs, rebuilding the trigger as the triggerer does, and resuming
on a fresh operator instance."""
import importlib
import json
from unittest.mock import MagicMock, patch

import pytest

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS, TaskDeferred
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.operator import SparkForensicsOperator

from ._local_ssh import SAMPLE_JSON, LocalHost, needs_posix_host, pid_alive, run_trigger, ti_context, wait_until

pytest.importorskip("airflow.providers.ssh.triggers.ssh_remote_job")

pytestmark = needs_posix_host

CONN = "onprem_ssh"


@pytest.fixture
def host(tmp_path):
    host = LocalHost(tmp_path)
    host.fake_cli(f"""\
        printf '%s' '{json.dumps(SAMPLE_JSON)}' | emit
        echo '[violation] max-spill: 3.1 GB > 2 GB' >&2
        echo '[inconclusive] min-efficiency: no executor metrics' >&2
        exit 1
    """)
    with host.patched():
        yield host


def _operator(report_dest, deferrable, notifier, on_threshold_breach, **extra):
    return SparkForensicsOperator(
        task_id="forensics",
        # Templated arguments: what crosses the deferral must be the
        # rendered values.
        log_source=RemotePathLogSourceHook(ssh_conn_id="{{ 'onprem' }}_ssh", path_template="/logs/{{ run_id }}"),
        backend=SSHAnalyzeHook(ssh_conn_id="{{ 'onprem' }}_ssh", poll_interval=0.1),
        report_dest=report_dest,
        max_spill_gb=2,
        min_efficiency_pct=50,
        on_threshold_breach=on_threshold_breach,
        notifier=notifier,
        deferrable=deferrable,
        **extra,
    )


def _round_trip(value):
    if AIRFLOW_V3_PLUS:
        try:
            from airflow.sdk.serde import deserialize, serialize
        except ImportError:  # Airflow 3.0/3.1 keep serde in core.
            from airflow.serialization.serde import deserialize, serialize

        return deserialize(json.loads(json.dumps(serialize(value))))
    from airflow.serialization.serialized_objects import BaseSerialization

    return BaseSerialization.deserialize(json.loads(json.dumps(BaseSerialization.serialize(value))))


def _run_sync(report_dest, notifier, on_threshold_breach, context, extra):
    op = _operator(report_dest, False, notifier, on_threshold_breach, **extra)
    op.render_template_fields(context)
    try:
        return op, op.execute(context), None
    except ThresholdBreached as e:
        return op, None, e


def _run_deferred(report_dest, notifier, on_threshold_breach, context, extra):
    first = _operator(report_dest, True, MagicMock(), on_threshold_breach, **extra)
    first.render_template_fields(context)
    with pytest.raises(TaskDeferred) as deferred:
        first.execute(context)
    classpath, trigger_kwargs = deferred.value.trigger.serialize()
    module, _, name = classpath.rpartition(".")
    trigger = getattr(importlib.import_module(module), name)(**_round_trip(trigger_kwargs))
    event = run_trigger(trigger)
    next_kwargs = {"event": event, **_round_trip(deferred.value.kwargs)}

    fresh = _operator(report_dest, True, notifier, on_threshold_breach, **extra)
    fresh.render_template_fields(context)
    try:
        return fresh, fresh.resume_execution(deferred.value.method_name, next_kwargs, context), None
    except ThresholdBreached as e:
        return fresh, None, e


def _without_out(argv):
    """The CLI arguments apart from --out, the one intended difference:
    the deferred job writes its report to a file, the synchronous run to
    stdout."""
    if "--out" not in argv:
        return argv
    i = argv.index("--out")
    return argv[:i] + argv[i + 2:]


def _outcomes(host, tmp_path, on_threshold_breach, **extra):
    """Runs the same operator input synchronously and deferred; what each
    mode produced, with its own directory normalized away."""
    outcomes = {}
    for mode, run in (("sync", _run_sync), ("deferred", _run_deferred)):
        context = ti_context(run_id="manual__2026-01-01")
        report_dest = str(tmp_path / mode / "report.json")
        notifier = MagicMock()
        op, result, breach = run(report_dest, notifier, on_threshold_breach, context, extra)

        def norm(path, mode_dir=str(tmp_path / mode)):
            return path and path.replace(mode_dir, "<dir>")

        [(report, destination)] = [c.args for c in notifier.notify.call_args_list]
        outcomes[mode] = {
            "persisted": (tmp_path / mode / "report.json").read_text(),
            "returned": norm(result),
            "breach": breach and str(breach),
            "notified_report": report,
            "notified_destination": norm(destination),
            "link": norm(ReportLink().get_link(op, ti_key=None)) if AIRFLOW_V3_PLUS else None,
            "cli_argv": _without_out(host.argv()),
            "summary": {k: norm(v) if isinstance(v, str) else v for k, v in context["ti"].pushed["sparkforensics_summary"].items()},
        }
    return outcomes


@pytest.mark.parametrize("on_threshold_breach", ["warn", "fail"])
def test_deferred_and_synchronous_runs_give_identical_results(host, tmp_path, on_threshold_breach):
    outcomes = _outcomes(host, tmp_path, on_threshold_breach)

    assert outcomes["deferred"] == outcomes["sync"]
    persisted = json.loads(outcomes["sync"]["persisted"])
    assert persisted["findings"] == SAMPLE_JSON["findings"]
    assert {r["name"]: r["status"] for r in persisted["thresholdResults"]} == {
        "max-spill": "violation",
        "min-efficiency": "inconclusive",
    }
    assert (outcomes["sync"]["breach"] is not None) == (on_threshold_breach == "fail")
    assert outcomes["sync"]["cli_argv"][0] == "/logs/manual__2026-01-01"
    assert outcomes["sync"]["summary"]["breached_thresholds"] == ["max-spill"]
    # Nothing left on the host.
    assert not any((host.home / ".sparkforensics" / "jobs").iterdir())


COMPARISON = {
    "confidence": "low",
    "reason": "Run names differ, so deltas may compare different work.",
    "matchedCoverage": 0.8,
    "metrics": [{"key": "wallClock", "baseline": 1000, "candidate": 1300, "delta": 300, "direction": "regression"}],
    "findings": {"introduced": [], "resolved": []},
}


@pytest.mark.parametrize("on_threshold_breach", ["warn", "fail"])
def test_deferred_and_synchronous_baseline_runs_give_identical_results(tmp_path, on_threshold_breach):
    host = LocalHost(tmp_path)
    wrapped = json.dumps({"candidate": SAMPLE_JSON, "comparison": COMPARISON})
    host.fake_cli(f"""\
        printf '%s' '{wrapped}' | emit
        echo '[violation] max-regression: Metric "wallClock" regressed 30.0%, exceeding budget 20%.' >&2
        exit 1
    """)
    with host.patched():
        outcomes = _outcomes(
            host, tmp_path, on_threshold_breach,
            # Templated like log_source: the rendered path crosses the deferral.
            baseline_log_source=RemotePathLogSourceHook(
                ssh_conn_id="{{ 'onprem' }}_ssh", path_template="/baselines/{{ run_id }}"
            ),
            max_regression_pct=20,
            fail_on_introduced="critical",
        )

    assert outcomes["deferred"] == outcomes["sync"]
    sync = outcomes["sync"]
    assert sync["cli_argv"][:5] == ["/logs/manual__2026-01-01", "--format", "json", "--baseline", "/baselines/manual__2026-01-01"]
    assert json.loads(sync["persisted"])["comparison"] == COMPARISON
    assert sync["summary"]["breached_thresholds"] == ["max-regression"]
    assert sync["summary"]["comparison"] == {
        "verdict": "regression",
        "confidence": "low",
        "reason": "Run names differ, so deltas may compare different work.",
        "metric": "wallClock",
        "baseline": 1000,
        "candidate": 1300,
        "regression_pct": 30.0,
    }
    assert (sync["breach"] is not None) == (on_threshold_breach == "fail")
    assert not any((host.home / ".sparkforensics" / "jobs").iterdir())


@pytest.mark.parametrize("procps", [True, False], ids=["pkill", "no-procps"])
def test_killing_a_deferrable_task_while_it_runs_stops_the_remote_job(tmp_path, procps):
    host = LocalHost(tmp_path)
    if not procps:
        host.hide_procps()
    host.fake_cli(f"echo $$ > {host.home}/cli_pid; sleep 30\n")
    with host.patched():
        op = _operator(str(tmp_path / "report.json"), True, None, "fail")
        context = ti_context(run_id="manual__2026-01-01")
        op.render_template_fields(context)
        with pytest.raises(TaskDeferred):
            op.execute(context)
        assert wait_until(lambda: (host.home / "cli_pid").exists())
        pid = int((host.home / "cli_pid").read_text())

        op.on_kill()

    assert wait_until(lambda: not pid_alive(pid))
    assert not any((host.home / ".sparkforensics" / "jobs").iterdir())


@pytest.mark.parametrize("procps", [True, False], ids=["pkill", "no-procps"])
def test_a_fresh_operator_killed_on_resume_stops_the_job_it_submitted(tmp_path, procps):
    # Airflow 2 calls on_kill() on the operator it resumes with, and nothing
    # else, when execution_timeout ran out during the deferral; its
    # remote_base_dir renders differently from the submitting try's here.
    host = LocalHost(tmp_path)
    if not procps:
        host.hide_procps()
    host.fake_cli(f"echo $$ > {host.home}/cli_pid; sleep 30 & echo $! > {host.home}/child_pid; wait\n")

    def operator():
        return SparkForensicsOperator(
            task_id="forensics",
            log_source=RemotePathLogSourceHook(ssh_conn_id=CONN, path_template="/logs/{{ run_id }}"),
            backend=SSHAnalyzeHook(
                ssh_conn_id=CONN, poll_interval=0.1,
                remote_base_dir=str(tmp_path) + "/jobs-{{ ti.try_number }}",
            ),
            report_dest=str(tmp_path / "report.json"),
            deferrable=True,
        )

    with host.patched():
        submitting = operator()
        context = ti_context(try_number=1)
        submitting.render_template_fields(context)
        with pytest.raises(TaskDeferred):
            submitting.execute(context)
        assert wait_until(lambda: (host.home / "child_pid").exists())
        pids = [int((host.home / name).read_text()) for name in ("cli_pid", "child_pid")]

        fresh = operator()
        context["ti"].try_number = 2
        fresh.render_template_fields(context)
        with patch("sparkforensics_operator.operator.current_context", return_value=context):
            fresh.on_kill()

    for pid in pids:
        assert wait_until(lambda: not pid_alive(pid)), f"{pid} still running"
    assert not any((tmp_path / "jobs-1").iterdir())
    assert not (tmp_path / "jobs-2").exists()
