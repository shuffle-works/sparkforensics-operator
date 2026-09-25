"""SparkForensicsOperator(deferrable=True) against a fake
DeferrableAnalyzeHook: runs without the ssh extra, so the deferral logic is
covered at the Airflow 2.6 floor too. The SSH backend end to end is in
tests/hooks/analyze/test_deferrable_parity.py."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator import spark_forensics_callback
from sparkforensics_operator._compat import AIRFLOW_V3_PLUS, BaseOperator, TaskDeferred
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze.base import DeferrableAnalyzeHook
from sparkforensics_operator.hooks.analyze.subprocess import SubprocessAnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, RemoteEventLog
from sparkforensics_operator.operator import SparkForensicsOperator
from sparkforensics_operator.report import Report, ThresholdResult

LOG = RemoteEventLog("onprem_ssh", "/logs/app-1")

# Airflow < 2.7 resumes a deferred task without BaseOperator.resume_execution.
# Nothing defers there in practice: the ssh extra needs Airflow >= 2.11.
needs_resume_execution = pytest.mark.skipif(
    not hasattr(BaseOperator, "resume_execution"),
    reason="BaseOperator.resume_execution is Airflow >= 2.7",
)


def _report(*threshold_results, exit_code=0):
    return Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[], threshold_results=list(threshold_results),
        exit_code=exit_code,
    )


class FakeDeferrableHook(DeferrableAnalyzeHook):
    supported_log_refs = (RemoteEventLog, HistoryServerApp)

    def __init__(self, report=None):
        super().__init__()
        self.report = report or _report()
        self.submitted = []
        self.collected = []
        self.abandoned = []

    def _analyze(self, log_ref, thresholds):
        return self.report

    def submit(self, log_ref, thresholds, context):
        self.submitted.append((log_ref, thresholds))
        return {"job_id": "job-1", "timeout": 900}

    def trigger_for(self, job):
        return ("trigger", job["job_id"])

    def defer_timeout(self, job):
        return timedelta(seconds=job["timeout"] + 120)

    def collect(self, job, event, log_ref, thresholds):
        self.collected.append((job, event, log_ref, thresholds))
        return self.report

    def abandon(self, context):
        self.abandoned.append(context)


def _operator(backend, tmp_path, **kwargs):
    log_source = MagicMock()
    log_source.resolve.return_value = LOG
    kwargs.setdefault("deferrable", True)
    return SparkForensicsOperator(
        task_id="forensics", log_source=log_source, backend=backend,
        report_dest=str(tmp_path / "report.json"), **kwargs,
    )


def _serialize_round_trip(value):
    """What Airflow does to a deferral's resume kwargs between the task
    deferring and it resuming on a worker."""
    if AIRFLOW_V3_PLUS:
        try:
            from airflow.sdk.serde import deserialize, serialize
        except ImportError:  # Airflow 3.0/3.1 keep serde in core.
            from airflow.serialization.serde import deserialize, serialize

        return deserialize(json.loads(json.dumps(serialize(value))))
    from airflow.serialization.serialized_objects import BaseSerialization

    return BaseSerialization.deserialize(json.loads(json.dumps(BaseSerialization.serialize(value))))


def _defer(op, context=None):
    with pytest.raises(TaskDeferred) as deferred:
        op.execute(context or {})
    return deferred.value


def _event(**overrides):
    return {"job_id": "job-1", "done": True, "exit_code": 0, "log_chunk": "", **overrides}


def test_deferrable_execute_submits_and_defers_with_everything_resume_needs(tmp_path):
    backend = FakeDeferrableHook()
    op = _operator(backend, tmp_path, max_spill_gb=2)

    deferred = _defer(op)

    assert backend.submitted == [(LOG, op.thresholds)]
    assert deferred.trigger == ("trigger", "job-1")
    assert deferred.method_name == "execute_complete"
    assert deferred.timeout == timedelta(seconds=1020)
    assert deferred.kwargs == {
        "job": {"job_id": "job-1", "timeout": 900},
        "log_ref": {"kind": "remote", "ssh_conn_id": "onprem_ssh", "path": "/logs/app-1"},
        "report_dest": str(tmp_path / "report.json"),
        "thresholds": op.thresholds,
    }


def test_deferral_ends_at_the_tasks_execution_timeout_when_that_comes_first(tmp_path):
    # Airflow 3 does not cap a deferral's timeout at execution_timeout the
    # way Airflow 2 does, so the operator has to.
    op = _operator(FakeDeferrableHook(), tmp_path, execution_timeout=timedelta(seconds=15))
    ti = MagicMock(start_date=datetime.now(timezone.utc) - timedelta(seconds=5))

    deferred = _defer(op, {"ti": ti})

    assert timedelta(seconds=9) < deferred.timeout <= timedelta(seconds=10)


def test_backend_defer_timeout_applies_when_execution_timeout_is_later(tmp_path):
    op = _operator(FakeDeferrableHook(), tmp_path, execution_timeout=timedelta(hours=1))
    ti = MagicMock(start_date=datetime.now(timezone.utc))

    assert _defer(op, {"ti": ti}).timeout == timedelta(seconds=1020)


@needs_resume_execution
def test_resume_on_a_fresh_operator_persists_the_report_from_serialized_kwargs(tmp_path):
    first = _operator(FakeDeferrableHook(), tmp_path, max_spill_gb=2)
    kwargs = _serialize_round_trip(_defer(first).kwargs)
    # Airflow rebuilds the operator from the DAG file to resume it; nothing
    # set on the first instance survives.
    backend = FakeDeferrableHook()
    fresh = _operator(backend, tmp_path, max_spill_gb=2)

    result = fresh.resume_execution("execute_complete", {"event": _event(), **kwargs}, {})

    assert result == str(tmp_path / "report.json")
    assert json.loads((tmp_path / "report.json").read_text())["schemaVersion"] == 3
    assert fresh.persisted_report_dest == result
    [(job, event, log_ref, thresholds)] = backend.collected
    assert log_ref == LOG
    assert thresholds == first.thresholds


def test_resume_uses_the_submitted_report_dest_and_thresholds_not_the_edited_dag(tmp_path):
    kwargs = _defer(_operator(FakeDeferrableHook(), tmp_path, max_spill_gb=2)).kwargs
    backend = FakeDeferrableHook()
    edited = SparkForensicsOperator(
        task_id="forensics", log_source=MagicMock(), backend=backend,
        report_dest=str(tmp_path / "edited.json"), max_spill_gb=99, deferrable=True,
    )

    result = edited.execute_complete({}, event=_event(), **kwargs)

    assert result == str(tmp_path / "report.json")
    assert backend.collected[0][3]["max_spill_gb"] == 2


def test_resume_raises_threshold_breached_and_still_records_the_report(tmp_path):
    report = _report(ThresholdResult("max-spill", "violation", "3 GB > 2 GB"), exit_code=1)
    kwargs = _defer(_operator(FakeDeferrableHook(report), tmp_path)).kwargs
    notifier = MagicMock()
    fresh = _operator(FakeDeferrableHook(report), tmp_path, notifier=notifier)

    with pytest.raises(ThresholdBreached, match="max-spill: 3 GB > 2 GB"):
        fresh.execute_complete({}, event=_event(exit_code=1), **kwargs)

    assert fresh.persisted_report_dest == str(tmp_path / "report.json")
    notifier.notify.assert_called_once()


def test_resume_cleans_up_the_log_source_even_when_collect_fails(tmp_path):
    kwargs = _defer(_operator(FakeDeferrableHook(), tmp_path)).kwargs
    backend = FakeDeferrableHook()
    backend.collect = MagicMock(side_effect=AirflowException("exit 2"))
    fresh = _operator(backend, tmp_path)

    with pytest.raises(AirflowException, match="exit 2"):
        fresh.execute_complete({}, event=_event(exit_code=2), **kwargs)

    fresh.log_source.cleanup.assert_called_once_with(LOG)


def test_resume_logs_the_last_remote_log_chunk(tmp_path, caplog):
    kwargs = _defer(_operator(FakeDeferrableHook(), tmp_path)).kwargs
    fresh = _operator(FakeDeferrableHook(), tmp_path)

    with patch.object(fresh.log, "info") as info:
        fresh.execute_complete({}, event=_event(log_chunk="line one\nline two"), **kwargs)

    info.assert_any_call("[remote] %s", "line one")
    info.assert_any_call("[remote] %s", "line two")


def test_resume_without_an_event_fails_clearly(tmp_path):
    kwargs = _defer(_operator(FakeDeferrableHook(), tmp_path)).kwargs

    with pytest.raises(AirflowException, match="without a trigger event"):
        _operator(FakeDeferrableHook(), tmp_path).execute_complete({}, event=None, **kwargs)


@needs_resume_execution
@pytest.mark.parametrize("error", ["Trigger/execution timeout", "Trigger failure"])
def test_a_failed_or_timed_out_deferral_abandons_the_remote_job_before_failing(tmp_path, error):
    backend = FakeDeferrableHook()
    fresh = _operator(backend, tmp_path)
    context = {"ti": MagicMock()}

    with pytest.raises(Exception, match=error):
        fresh.resume_execution("__fail__", {"error": error}, context)

    assert backend.abandoned == [context]


@needs_resume_execution
def test_an_abandon_failure_does_not_hide_the_deferral_error(tmp_path):
    backend = FakeDeferrableHook()
    backend.abandon = MagicMock(side_effect=AirflowException("host unreachable"))

    with pytest.raises(Exception, match="Trigger/execution timeout"):
        _operator(backend, tmp_path).resume_execution(
            "__fail__", {"error": "Trigger/execution timeout"}, {}
        )


def test_submit_failure_cleans_up_the_log_source_and_does_not_defer(tmp_path):
    backend = FakeDeferrableHook()
    backend.submit = MagicMock(side_effect=AirflowException("Connection refused"))
    op = _operator(backend, tmp_path)

    with pytest.raises(AirflowException, match="Connection refused"):
        op.execute({})

    op.log_source.cleanup.assert_called_once_with(LOG)


def test_deferrable_rejects_a_log_the_backend_cannot_read_before_submitting(tmp_path):
    from pathlib import Path

    from sparkforensics_operator.log_ref import LocalEventLog

    backend = FakeDeferrableHook()
    op = _operator(backend, tmp_path)
    op.log_source.resolve.return_value = LocalEventLog(Path("/tmp/app.log"))

    with pytest.raises(AirflowException, match="cannot analyze"):
        op.execute({})

    assert backend.submitted == []


def test_deferrable_true_rejects_a_backend_that_cannot_defer():
    with pytest.raises(ValueError, match="deferrable=True needs a backend.*SubprocessAnalyzeHook"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=SubprocessAnalyzeHook(),
            report_dest="/tmp/r.json", deferrable=True,
        )


def test_default_deferrable_config_applies_only_to_a_backend_that_can_defer(tmp_path):
    with patch("sparkforensics_operator.operator.conf.getboolean", return_value=True):
        deferring = _operator(FakeDeferrableHook(), tmp_path, deferrable=None)
        local = _operator(SubprocessAnalyzeHook(), tmp_path, deferrable=None)

    assert deferring.deferrable is True
    assert local.deferrable is False


def test_deferrable_defaults_off(tmp_path):
    assert _operator(FakeDeferrableHook(), tmp_path, deferrable=None).deferrable is False


def test_deferrable_false_runs_a_deferrable_backend_synchronously(tmp_path):
    backend = FakeDeferrableHook()
    op = _operator(backend, tmp_path, deferrable=False)

    assert op.execute({}) == str(tmp_path / "report.json")
    assert backend.submitted == []


def test_the_callback_form_rejects_deferrable():
    with pytest.raises(ValueError, match="spark_forensics_callback cannot defer"):
        spark_forensics_callback(
            log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json", deferrable=True
        )


def test_on_kill_while_submitting_or_resuming_abandons_the_remote_job(tmp_path):
    backend = FakeDeferrableHook()
    op = _operator(backend, tmp_path)
    context = {"ti": MagicMock()}
    with pytest.raises(TaskDeferred):
        op.execute(context)

    op.on_kill()

    assert backend.abandoned == [context]


def test_on_kill_on_a_resumed_operator_abandons_with_its_own_context(tmp_path):
    kwargs = _defer(_operator(FakeDeferrableHook(), tmp_path)).kwargs
    backend = FakeDeferrableHook()
    backend.collect = MagicMock(side_effect=lambda *a: fresh.on_kill() or backend.report)
    fresh = _operator(backend, tmp_path)
    context = {"ti": MagicMock()}

    fresh.execute_complete(context, event=_event(), **kwargs)

    assert backend.abandoned == [context]


def test_on_kill_of_a_synchronous_run_asks_the_backend_to_stop(tmp_path):
    backend = FakeDeferrableHook()
    backend.on_kill = MagicMock()
    op = _operator(backend, tmp_path, deferrable=False)
    op.execute({})

    op.on_kill()

    backend.on_kill.assert_called_once_with()
    assert backend.abandoned == []


def test_on_kill_before_execute_does_nothing(tmp_path):
    backend = FakeDeferrableHook()
    backend.on_kill = MagicMock()

    _operator(backend, tmp_path).on_kill()

    assert backend.abandoned == []
    backend.on_kill.assert_not_called()


def test_on_kill_failure_is_logged_not_raised(tmp_path):
    backend = FakeDeferrableHook()
    backend.abandon = MagicMock(side_effect=AirflowException("host unreachable"))
    op = _operator(backend, tmp_path)
    with pytest.raises(TaskDeferred):
        op.execute({})

    with patch.object(op.log, "warning") as warning:
        op.on_kill()

    warning.assert_called_once()
