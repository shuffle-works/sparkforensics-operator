import json
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException

from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze._cli import build_report
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog
from sparkforensics_operator.operator import SparkForensicsOperator, run_spark_forensics
from sparkforensics_operator.report import Report, ThresholdResult


def _report(*threshold_results, exit_code=0):
    return Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[], threshold_results=list(threshold_results),
        exit_code=exit_code,
    )


def _fixtures(tmp_path, report):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = report
    return log_source, backend


def test_run_spark_forensics_persists_the_report_and_returns_the_destination(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    dest = tmp_path / "report.json"

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    assert result == str(dest)
    assert dest.exists()


def test_run_spark_forensics_raises_threshold_breached_on_violation_when_failing(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with pytest.raises(ThresholdBreached, match="max-runtime"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )


def test_run_spark_forensics_attaches_the_persisted_destination_to_threshold_breached(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )
    dest = tmp_path / "report.json"

    with pytest.raises(ThresholdBreached) as exc_info:
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(dest),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    # The report was already persisted successfully before this raise; callers
    # that can't get a return value on this path (spark_forensics_callback's
    # _callback) recover the destination from the exception to still push it
    # to XCom (see callback.py).
    assert exc_info.value.destination == str(dest)
    assert dest.exists()


def test_run_spark_forensics_warns_instead_of_raising_when_configured_to_warn(tmp_path, caplog):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with caplog.at_level(logging.WARNING):
        result = run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="warn", notifier=None,
            log=logging.getLogger("test"),
        )

    assert result == str(tmp_path / "report.json")
    assert any("max-runtime" in message for message in caplog.messages)


def test_run_spark_forensics_ignores_violations_when_configured_to_ignore(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={"max_runtime_ms": 1000}, on_threshold_breach="ignore", notifier=None,
        log=logging.getLogger("test"),
    )

    assert result == str(tmp_path / "report.json")


def test_run_spark_forensics_calls_the_notifier_when_configured(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    notifier = MagicMock()

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=notifier, log=logging.getLogger("test"),
    )

    notifier.notify.assert_called_once_with(backend.analyze.return_value, result)


def test_run_spark_forensics_notifies_even_when_a_breach_raises(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )
    notifier = MagicMock()

    with pytest.raises(ThresholdBreached, match="max-runtime"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=notifier,
            log=logging.getLogger("test"),
        )

    notifier.notify.assert_called_once()


def test_run_spark_forensics_persists_the_full_cli_shape_including_evidence_and_thresholds(tmp_path):
    report = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
        evidence_availability={"taskLevel": False}, detectors=["spill"],
        threshold_results=[ThresholdResult("max-runtime", "pass", "")],
    )
    log_source, backend = _fixtures(tmp_path, report)
    dest = tmp_path / "report.json"

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
        log=logging.getLogger("test"),
    )

    persisted = json.loads(dest.read_text())
    assert persisted["evidenceAvailability"] == {"taskLevel": False}
    assert persisted["detectors"] == ["spill"]
    assert persisted["thresholdResults"] == [{"name": "max-runtime", "status": "pass", "detail": ""}]


def test_run_spark_forensics_persists_every_section_of_a_real_cli_report(tmp_path):
    # Real sparkforensics-cli 0.4.0 output: the persisted report is the CLI's
    # own report plus thresholdResults, with no section dropped.
    fixtures = Path(__file__).parent / "fixtures" / "sparkforensics_cli_0_4_0"
    cli_report = (fixtures / "report.json").read_text()
    thresholds = {"max_runtime_ms": 1, "max_skew_ratio": 2, "max_spill_gb": 100}
    report = build_report(cli_report, thresholds, (fixtures / "report.stderr").read_text(), 1)
    log_source, backend = _fixtures(tmp_path, report)
    dest = tmp_path / "report.json"

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds=thresholds, on_threshold_breach="ignore", notifier=None,
        log=logging.getLogger("test"),
    )

    persisted = json.loads(dest.read_text())
    thresholds_results = persisted.pop("thresholdResults")
    assert persisted == json.loads(cli_report)
    assert {r["name"]: r["status"] for r in thresholds_results} == {
        "max-runtime": "violation", "max-skew": "violation", "max-spill": "pass",
    }


def test_run_spark_forensics_threads_aws_conn_id_to_sinks_persist(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    with patch("sparkforensics_operator.operator.sinks.persist", return_value="s3://bucket/key") as mock_persist:
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest="s3://bucket/key",
            thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
            aws_conn_id="my_aws_conn",
        )

    assert mock_persist.call_args.kwargs["aws_conn_id"] == "my_aws_conn"


def test_run_spark_forensics_calls_cleanup_on_the_log_source_after_success(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    log_source.cleanup.assert_called_once_with(log_source.locate.return_value)


def test_run_spark_forensics_calls_cleanup_even_when_analyze_raises(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
        )

    log_source.cleanup.assert_called_once_with(LocalEventLog(tmp_path / "app.log"))


def test_run_spark_forensics_calls_cleanup_even_when_a_breach_raises(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with pytest.raises(ThresholdBreached):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    log_source.cleanup.assert_called_once_with(log_source.locate.return_value)


def test_run_spark_forensics_treats_exit_code_1_with_no_parsed_violation_as_a_breach(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report(exit_code=1))

    with pytest.raises(ThresholdBreached, match="exit code 1"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )


def test_run_spark_forensics_warns_on_exit_code_3_with_no_parsed_inconclusive_result(tmp_path, caplog):
    log_source, backend = _fixtures(tmp_path, _report(exit_code=3))

    with caplog.at_level(logging.WARNING):
        result = run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    # Log-only fallback: no ThresholdBreached, return value unchanged, even
    # with on_threshold_breach="fail" -- exit code 3 is not a violation.
    assert result == str(tmp_path / "report.json")
    assert any("exit code 3" in message for message in caplog.messages)


def test_operator_execute_delegates_to_run_spark_forensics(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    op = SparkForensicsOperator(
        task_id="run_forensics", log_source=log_source, backend=backend,
        report_dest=str(tmp_path / "report.json"),
    )

    result = op.execute({})

    assert result == str(tmp_path / "report.json")


def test_operator_execute_passes_aws_conn_id_to_sinks_persist(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    op = SparkForensicsOperator(
        task_id="run_forensics", log_source=log_source, backend=backend,
        report_dest="s3://bucket/key", aws_conn_id="my_aws_conn",
    )

    with patch("sparkforensics_operator.operator.sinks.persist", return_value="s3://bucket/key") as mock_persist:
        op.execute({})

    assert mock_persist.call_args.kwargs["aws_conn_id"] == "my_aws_conn"


def test_operator_rejects_an_invalid_on_threshold_breach_value():
    with pytest.raises(ValueError, match="on_threshold_breach"):
        SparkForensicsOperator(
            task_id="run_forensics", log_source=MagicMock(), backend=MagicMock(),
            report_dest="/tmp/report.json", on_threshold_breach="explode",
        )


def test_run_spark_forensics_rejects_an_invalid_on_threshold_breach_value(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    with pytest.raises(ValueError, match="on_threshold_breach"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="explode", notifier=None,
            log=logging.getLogger("test"),
        )


def test_operator_registers_the_report_link():
    from sparkforensics_operator.links import ReportLink

    assert any(isinstance(link, ReportLink) for link in SparkForensicsOperator.operator_extra_links)


def test_operator_templates_report_dest():
    with DAG(dag_id="sparkforensics_templating", start_date=datetime(2026, 1, 1), schedule=None):
        task = SparkForensicsOperator(
            task_id="run_forensics",
            log_source=MagicMock(),
            backend=MagicMock(),
            report_dest="/tmp/sparkforensics/{{ run_id }}/report.json",
        )

    assert "report_dest" in task.template_fields

    rendered = task.render_template(task.report_dest, {"run_id": "abc123"})

    assert "{{" not in rendered
    assert rendered == "/tmp/sparkforensics/abc123/report.json"


def test_run_spark_forensics_hands_a_non_local_log_ref_to_the_backend_unchanged(tmp_path):
    # A reference the worker never fetched (e.g. a History Server app the
    # backend reads itself) flows from locate() to analyze() and cleanup().
    log_ref = HistoryServerApp(base_url="http://localhost:18080", app_id="app-1")
    log_source = MagicMock()
    log_source.locate.return_value = log_ref
    backend = MagicMock()
    backend.analyze.return_value = _report()

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    backend.analyze.assert_called_once_with(log_ref, {})
    log_source.cleanup.assert_called_once_with(log_ref)


# Comparing against a baseline run.

COMPARISON = {
    "confidence": "ok", "reason": None, "matchedCoverage": 1,
    "metrics": [{"key": "wallClock", "baseline": 1000, "candidate": 1300, "delta": 300, "direction": "regression"}],
    "findings": {"introduced": [], "resolved": []},
}


def _baseline_fixtures(tmp_path, report):
    log_source, backend = _fixtures(tmp_path, report)
    baseline_log_source = MagicMock()
    baseline_log_source.locate.return_value = LocalEventLog(tmp_path / "baseline.log")
    return log_source, backend, baseline_log_source


@pytest.mark.parametrize("threshold", [
    {"max_regression_pct": 20},
    {"max_regression_pct": 20, "regression_metric": "gcTime"},
    {"fail_on_introduced": "critical"},
])
def test_operator_rejects_a_comparison_threshold_without_a_baseline_log_source(threshold):
    with pytest.raises(ValueError, match="need baseline_log_source"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json", **threshold,
        )


def test_operator_rejects_a_regression_metric_without_max_regression_pct():
    with pytest.raises(ValueError, match="regression_metric needs max_regression_pct"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
            baseline_log_source=MagicMock(), regression_metric="gcTime",
        )


def test_operator_rejects_an_unknown_fail_on_introduced_band():
    with pytest.raises(ValueError, match="fail_on_introduced must be one of"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
            baseline_log_source=MagicMock(), fail_on_introduced="severe",
        )


def _operator_with(**kwargs):
    return SparkForensicsOperator(
        task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
        baseline_log_source=kwargs.pop("baseline_log_source", MagicMock()), **kwargs,
    )


@pytest.mark.parametrize("threshold", [
    {"regression_budgets": {"gcTime": 10}},
    {"stage_regression_budgets": {"executorRunTime": 30}},
    {"stage_regression_budgets": {"executorRunTime": 30}, "stage_qualities": ["exact"]},
])
def test_operator_rejects_a_regression_budget_without_a_baseline_log_source(threshold):
    with pytest.raises(ValueError, match="need baseline_log_source"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json", **threshold,
        )


@pytest.mark.parametrize("empty", [{}, [], None])
def test_operator_treats_an_empty_budget_as_none_so_it_needs_no_baseline(empty):
    op = SparkForensicsOperator(
        task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
        regression_budgets=empty, stage_regression_budgets=empty,
    )

    assert not op.thresholds["regression_budgets"]


def test_operator_keeps_the_regression_budgets_in_its_thresholds_with_the_qualities_as_a_list():
    op = _operator_with(
        regression_budgets={"gcTime": 10}, stage_regression_budgets={"executorRunTime": 30},
        stage_qualities=("exact", "aligned"),
    )

    assert op.thresholds["regression_budgets"] == {"gcTime": 10}
    assert op.thresholds["stage_regression_budgets"] == {"executorRunTime": 30}
    assert op.thresholds["stage_qualities"] == ["exact", "aligned"]


def test_operator_accepts_budgets_for_several_metrics_next_to_the_single_metric_pair():
    _operator_with(max_regression_pct=20, regression_metric="gcTime", regression_budgets={"wallClock": 10})
    _operator_with(max_regression_pct=20, regression_budgets={"gcTime": 10})


@pytest.mark.parametrize("kwargs", [
    {"max_regression_pct": 20, "regression_budgets": {"wallClock": 10}},
    {"max_regression_pct": 20, "regression_metric": "gcTime", "regression_budgets": {"gcTime": 10}},
])
def test_operator_rejects_a_metric_budgeted_twice(kwargs):
    with pytest.raises(ValueError, match="is budgeted twice"):
        _operator_with(**kwargs)


@pytest.mark.parametrize("budgets", [
    ["gcTime"],
    {"gcTime": -1},
    {"gcTime": "10"},
    {"gcTime": True},
    {"gcTime": float("inf")},
    {"gcTime": float("nan")},
    {"": 10},
    {"gc:Time": 10},
    {3: 10},
])
def test_operator_rejects_a_malformed_regression_budget(budgets):
    with pytest.raises(ValueError, match="regression_budgets"):
        _operator_with(regression_budgets=budgets)
    with pytest.raises(ValueError, match="stage_regression_budgets"):
        _operator_with(stage_regression_budgets=budgets)


@pytest.mark.parametrize("threshold", [
    {"normalize_path": ["/out/\\d+"]},
    {"ignore_conf": ["output"]},
    {"noise_band_pct": 5},
    {"noise_band_pct": 0},
])
def test_operator_rejects_comparison_tuning_without_a_baseline_log_source(threshold):
    with pytest.raises(ValueError, match="need baseline_log_source"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json", **threshold,
        )


def test_operator_keeps_the_comparison_tuning_in_its_thresholds_with_the_patterns_as_lists():
    op = _operator_with(normalize_path=("/out/\\d+",), ignore_conf=["output"], noise_band_pct=2.5)

    assert op.thresholds["normalize_path"] == ["/out/\\d+"]
    assert op.thresholds["ignore_conf"] == ["output"]
    assert op.thresholds["noise_band_pct"] == 2.5


@pytest.mark.parametrize("empty", [[], (), None])
def test_operator_treats_empty_comparison_patterns_as_none_so_they_need_no_baseline(empty):
    SparkForensicsOperator(
        task_id="forensics", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
        normalize_path=empty, ignore_conf=empty,
    )


@pytest.mark.parametrize("patterns", ["output", [""], [3], ["ok", None]])
def test_operator_rejects_malformed_comparison_patterns(patterns):
    with pytest.raises(ValueError, match="normalize_path must be a list of non-empty"):
        _operator_with(normalize_path=patterns)
    with pytest.raises(ValueError, match="ignore_conf must be a list of non-empty"):
        _operator_with(ignore_conf=patterns)


@pytest.mark.parametrize("noise_band", [-1, 100.5, "2", True, float("nan")])
def test_operator_rejects_a_noise_band_outside_0_to_100(noise_band):
    with pytest.raises(ValueError, match="noise_band_pct must be a number from 0 to 100"):
        _operator_with(noise_band_pct=noise_band)


def test_operator_rejects_stage_qualities_without_stage_regression_budgets():
    with pytest.raises(ValueError, match="stage_qualities needs stage_regression_budgets"):
        _operator_with(stage_qualities=["exact"])


@pytest.mark.parametrize("qualities", ["exact", ["exact", "fuzzy"], ("loose",)])
def test_operator_rejects_an_unknown_stage_quality(qualities):
    with pytest.raises(ValueError, match="stage_qualities must be a list drawn from"):
        _operator_with(stage_regression_budgets={"executorRunTime": 30}, stage_qualities=qualities)


def test_operator_rejects_a_baseline_for_a_backend_that_cannot_compare_runs():
    from sparkforensics_operator.hooks.analyze.base import AnalyzeHook

    class _NoBaselines(AnalyzeHook):
        supported_log_refs = (LocalEventLog,)

        def _analyze(self, log_ref, thresholds):
            return _report()

    with pytest.raises(ValueError, match="_NoBaselines cannot compare a run against a baseline"):
        SparkForensicsOperator(
            task_id="forensics", log_source=MagicMock(), backend=_NoBaselines(), report_dest="/tmp/r.json",
            baseline_log_source=MagicMock(),
        )


def test_run_spark_forensics_hands_the_located_baseline_to_the_backend_and_cleans_both_up(tmp_path):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    context = {"run_id": "r1"}

    run_spark_forensics(
        context, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={"max_regression_pct": 20}, on_threshold_breach="fail", notifier=None,
        log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
    )

    baseline_log_source.locate.assert_called_once_with(context)
    backend.analyze.assert_called_once_with(
        LocalEventLog(tmp_path / "app.log"), {"max_regression_pct": 20},
        baseline_ref=LocalEventLog(tmp_path / "baseline.log"),
    )
    baseline_log_source.cleanup.assert_called_once_with(LocalEventLog(tmp_path / "baseline.log"))
    log_source.cleanup.assert_called_once_with(LocalEventLog(tmp_path / "app.log"))


def test_run_spark_forensics_cleans_up_both_logs_when_the_backend_rejects_the_baseline(tmp_path):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    backend.analyze.side_effect = RuntimeError("cannot use it as the baseline")

    with pytest.raises(RuntimeError):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
        )

    baseline_log_source.cleanup.assert_called_once()
    log_source.cleanup.assert_called_once()


def test_run_spark_forensics_cleans_up_the_log_when_the_baseline_cannot_be_located(tmp_path):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    baseline_log_source.locate.side_effect = RuntimeError("no such baseline")

    with pytest.raises(RuntimeError, match="no such baseline"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
        )

    backend.analyze.assert_not_called()
    baseline_log_source.cleanup.assert_not_called()
    log_source.cleanup.assert_called_once()


def test_run_spark_forensics_refuses_a_baseline_staged_over_the_log_under_analysis(tmp_path):
    from airflow.exceptions import AirflowException

    from sparkforensics_operator.hooks.log_source.filesystem import FilesystemLogSourceHook

    for ds in ("2026-01-02", "2026-01-01"):
        (tmp_path / "logs" / ds).mkdir(parents=True)
        (tmp_path / "logs" / ds / "eventlog").write_text(ds)
    staging = str(tmp_path / "staging")
    backend = MagicMock()

    with pytest.raises(AirflowException, match="compared against itself"):
        run_spark_forensics(
            {}, log_source=FilesystemLogSourceHook(str(tmp_path / "logs/2026-01-02/eventlog"), dest_dir=staging),
            backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_regression_pct": 20}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
            baseline_log_source=FilesystemLogSourceHook(str(tmp_path / "logs/2026-01-01/eventlog"), dest_dir=staging),
        )

    backend.analyze.assert_not_called()
    assert not (tmp_path / "staging" / "eventlog").exists()


@pytest.mark.parametrize("baseline_path", ["logs/app", "logs/app/events_1_app", "logs"])
def test_run_spark_forensics_refuses_a_baseline_that_is_or_contains_the_log(tmp_path, baseline_path):
    from airflow.exceptions import AirflowException

    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    log_source.locate.return_value = LocalEventLog(tmp_path / "logs/app")
    baseline_log_source.locate.return_value = LocalEventLog(tmp_path / baseline_path)

    with pytest.raises(AirflowException, match="compared against itself"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
        )

    backend.analyze.assert_not_called()
    baseline_log_source.cleanup.assert_called_once()
    log_source.cleanup.assert_called_once()


def test_run_spark_forensics_accepts_a_sibling_baseline_with_a_shared_name_prefix(tmp_path):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    log_source.locate.return_value = LocalEventLog(tmp_path / "logs/app")
    baseline_log_source.locate.return_value = LocalEventLog(tmp_path / "logs/app-previous")

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None,
        log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
    )

    backend.analyze.assert_called_once()


def test_run_spark_forensics_hands_a_history_server_baseline_to_the_backend(tmp_path):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    baseline = HistoryServerApp(base_url="http://shs:18080", app_id="app-0")
    baseline_log_source.locate.return_value = baseline

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None,
        log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
    )

    backend.analyze.assert_called_once_with(LocalEventLog(tmp_path / "app.log"), {}, baseline_ref=baseline)
    baseline_log_source.cleanup.assert_called_once_with(baseline)


@pytest.mark.parametrize("baseline, overlaps", [
    (HistoryServerApp(base_url="http://shs:18080", app_id="app-1"), True),
    (HistoryServerApp(base_url="http://shs:18080", app_id="app-1", attempt_id="2"), False),
    (HistoryServerApp(base_url="http://shs:18080", app_id="app-0"), False),
    (HistoryServerApp(base_url="http://other-shs:18080", app_id="app-1"), False),
])
def test_a_history_server_baseline_is_refused_only_when_it_is_the_run_under_analysis(tmp_path, baseline, overlaps):
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, _report())
    log_source.locate.return_value = HistoryServerApp(base_url="http://shs:18080", app_id="app-1")
    baseline_log_source.locate.return_value = baseline

    def run():
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"), baseline_log_source=baseline_log_source,
        )

    if overlaps:
        with pytest.raises(AirflowException, match="compared against itself"):
            run()
        backend.analyze.assert_not_called()
    else:
        run()
        backend.analyze.assert_called_once()


def test_operator_fails_on_a_regression_breach_and_persists_the_comparison(tmp_path):
    report = _report(
        ThresholdResult("max-regression", "violation", 'Metric "wallClock" regressed 30.0%, exceeding budget 20%.'),
        exit_code=1,
    )
    report.comparison = COMPARISON
    log_source, backend, baseline_log_source = _baseline_fixtures(tmp_path, report)
    dest = tmp_path / "report.json"
    op = SparkForensicsOperator(
        task_id="forensics", log_source=log_source, backend=backend, report_dest=str(dest),
        baseline_log_source=baseline_log_source, max_regression_pct=20,
    )

    with pytest.raises(ThresholdBreached, match="max-regression: Metric \"wallClock\" regressed 30.0%"):
        op.execute({})

    persisted = json.loads(dest.read_text())
    assert persisted["comparison"] == COMPARISON
    assert persisted["thresholdResults"][0]["name"] == "max-regression"
    assert op.persisted_report_dest == str(dest)


def test_the_persisted_report_has_no_comparison_without_a_baseline(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    dest = tmp_path / "report.json"

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    assert "comparison" not in json.loads(dest.read_text())


def _run_with_cli_version(tmp_path, cli_version):
    report = _report()
    report.cli_version = cli_version
    log_source, backend = _fixtures(tmp_path, report)
    log = MagicMock()
    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=log,
    )
    return log


def test_a_run_by_a_cli_older_than_the_tested_version_logs_a_warning(tmp_path):
    log = _run_with_cli_version(tmp_path, "0.9.0")

    log.warning.assert_called_once()
    assert "sparkforensics-cli 0.9.0 is older than 0.10.1" in log.warning.call_args.args[1]


def test_a_run_by_the_tested_cli_version_logs_no_warning(tmp_path):
    log = _run_with_cli_version(tmp_path, "0.10.1")

    log.warning.assert_not_called()


def _persisted(tmp_path, report):
    log_source, backend = _fixtures(tmp_path, report)
    dest = tmp_path / "report.json"
    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={}, on_threshold_breach="ignore", notifier=None, log=MagicMock(),
    )
    return json.loads(dest.read_text())


def test_the_persisted_report_is_the_whole_cli_report_plus_the_threshold_results(tmp_path):
    cli_report = {
        "schemaVersion": 5,
        "summary": {"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        "verdict": None, "evidenceAvailability": None, "detectors": [], "findings": [],
        "recommendations": [], "cleanChecks": [], "notRunChecks": [],
        "generator": {"name": "sparkforensics-cli", "version": "0.10.1"},
        "app": {"id": "app-1", "attemptId": None},
        "metrics": {"schemaVersion": 1}, "effectiveConf": {"keys": []}, "writeTargets": {"writes": []},
    }
    report = _report(ThresholdResult("max-runtime", "pass", ""))
    report.raw = dict(cli_report)
    report.comparison = {"confidence": "ok"}

    persisted = _persisted(tmp_path, report)

    assert persisted == {
        **cli_report,
        "thresholdResults": [{"name": "max-runtime", "status": "pass", "detail": ""}],
        "comparison": {"confidence": "ok"},
    }
    assert report.raw == cli_report


def test_a_report_built_by_hand_persists_the_sections_the_operator_reads(tmp_path):
    persisted = _persisted(tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")))

    assert set(persisted) == {
        "schemaVersion", "summary", "verdict", "evidenceAvailability", "detectors", "findings",
        "recommendations", "cleanChecks", "notRunChecks", "thresholdResults",
    }
    assert persisted["thresholdResults"] == [{"name": "max-runtime", "status": "violation", "detail": "too slow"}]
