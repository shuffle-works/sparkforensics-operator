"""The same SparkForensicsOperator input, run synchronously and deferred
through SSHAnalyzeHook against the local stand-in SSH host (see
_local_ssh.py), must give identical results: persisted report, threshold
results, return value, ReportLink and notifier calls. The deferred run goes
through everything Airflow does in between: serializing the trigger and the
resume kwargs, rebuilding the trigger as the triggerer does, and resuming
on a fresh operator instance."""
import importlib
import json
from unittest.mock import MagicMock

import pytest

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.operator import SparkForensicsOperator

from ._local_ssh import SAMPLE_JSON, LocalHost, needs_posix_host, run_trigger, ti_context

pytest.importorskip("airflow.providers.ssh.triggers.ssh_remote_job")

try:
    from airflow.sdk.exceptions import TaskDeferred
except ImportError:  # Airflow 2
    from airflow.exceptions import TaskDeferred

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


def _operator(report_dest, deferrable, notifier, on_threshold_breach):
    return SparkForensicsOperator(
        task_id="forensics",
        log_source=RemotePathLogSourceHook(ssh_conn_id=CONN, path_template="/logs/{run_id}"),
        backend=SSHAnalyzeHook(ssh_conn_id=CONN, poll_interval=0.1),
        report_dest=report_dest,
        max_spill_gb=2,
        min_efficiency_pct=50,
        on_threshold_breach=on_threshold_breach,
        notifier=notifier,
        deferrable=deferrable,
    )


def _round_trip(value):
    if AIRFLOW_V3_PLUS:
        from airflow.sdk.serde import deserialize, serialize

        return deserialize(json.loads(json.dumps(serialize(value))))
    from airflow.serialization.serialized_objects import BaseSerialization

    return BaseSerialization.deserialize(json.loads(json.dumps(BaseSerialization.serialize(value))))


def _run_sync(report_dest, notifier, on_threshold_breach, context):
    op = _operator(report_dest, False, notifier, on_threshold_breach)
    try:
        return op, op.execute(context), None
    except ThresholdBreached as e:
        return op, None, e


def _run_deferred(report_dest, notifier, on_threshold_breach, context):
    first = _operator(report_dest, True, MagicMock(), on_threshold_breach)
    with pytest.raises(TaskDeferred) as deferred:
        first.execute(context)
    classpath, trigger_kwargs = deferred.value.trigger.serialize()
    module, _, name = classpath.rpartition(".")
    trigger = getattr(importlib.import_module(module), name)(**_round_trip(trigger_kwargs))
    event = run_trigger(trigger)
    next_kwargs = {"event": event, **_round_trip(deferred.value.kwargs)}

    fresh = _operator(report_dest, True, notifier, on_threshold_breach)
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


@pytest.mark.parametrize("on_threshold_breach", ["warn", "fail"])
def test_deferred_and_synchronous_runs_give_identical_results(host, tmp_path, on_threshold_breach):
    context = {**ti_context(), "run_id": "manual__2026-01-01"}
    outcomes = {}
    for mode, run in (("sync", _run_sync), ("deferred", _run_deferred)):
        report_dest = str(tmp_path / mode / "report.json")
        notifier = MagicMock()
        op, result, breach = run(report_dest, notifier, on_threshold_breach, context)

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
        }

    assert outcomes["deferred"] == outcomes["sync"]
    persisted = json.loads(outcomes["sync"]["persisted"])
    assert persisted["findings"] == SAMPLE_JSON["findings"]
    assert {r["name"]: r["status"] for r in persisted["thresholdResults"]} == {
        "max-spill": "violation",
        "min-efficiency": "inconclusive",
    }
    assert (outcomes["sync"]["breach"] is not None) == (on_threshold_breach == "fail")
    # Nothing left on the host.
    assert not any((host.home / ".sparkforensics" / "jobs").iterdir())
