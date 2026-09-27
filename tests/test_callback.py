from unittest.mock import MagicMock

import pytest

from sparkforensics_operator.callback import spark_forensics_callback
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.log_ref import LocalEventLog
from sparkforensics_operator.report import Report, ThresholdResult


def test_callback_factory_returns_a_callable_that_runs_the_shared_core(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
    )
    dest = tmp_path / "report.json"

    callback = spark_forensics_callback(log_source=log_source, backend=backend, report_dest=str(dest))
    callback({})

    assert dest.exists()


def test_callback_factory_accepts_threshold_kwargs_matching_the_operator(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
    )

    callback = spark_forensics_callback(
        log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        max_runtime_ms=10_000, on_threshold_breach="warn",
    )
    callback({})

    backend.analyze.assert_called_once_with(LocalEventLog(tmp_path / "app.log"), {
        "max_runtime_ms": 10_000, "max_spill_gb": None, "max_skew_ratio": None,
        "max_failed_task_rate_pct": None, "min_efficiency_pct": None,
        "max_regression_pct": None, "regression_metric": None, "fail_on_introduced": None,
    })


def test_callback_pushes_the_report_destination_to_xcom(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
    )
    dest = tmp_path / "report.json"

    callback = spark_forensics_callback(log_source=log_source, backend=backend, report_dest=str(dest))
    context = {"ti": MagicMock()}
    callback(context)

    context["ti"].xcom_push.assert_any_call(key="return_value", value=str(dest))


def test_callback_pushes_xcom_before_raising_on_a_breach(tmp_path):
    # on_threshold_breach="fail" is the callback's own default. On a breach,
    # run_spark_forensics raises ThresholdBreached *after* the report has
    # already been persisted; the callback path must still record where via
    # XCom (the one place a user most wants the report link), even though it
    # then lets the breach propagate as expected.
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-runtime", "violation", "too slow")],
    )
    dest = tmp_path / "report.json"

    callback = spark_forensics_callback(
        log_source=log_source, backend=backend, report_dest=str(dest),
        max_runtime_ms=1000,
    )
    context = {"ti": MagicMock()}

    with pytest.raises(ThresholdBreached, match="max-runtime"):
        callback(context)

    context["ti"].xcom_push.assert_any_call(key="return_value", value=str(dest))
    pushed = {c.kwargs["key"]: c.kwargs["value"] for c in context["ti"].xcom_push.call_args_list}
    assert pushed["sparkforensics_summary"]["breached_thresholds"] == ["max-runtime"]


def test_callback_raises_on_an_invalid_on_threshold_breach_value(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
    )

    callback = spark_forensics_callback(
        log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        on_threshold_breach="explode",
    )

    with pytest.raises(ValueError, match="on_threshold_breach"):
        callback({})


def test_callback_factory_rejects_a_comparison_threshold_without_a_baseline_log_source(tmp_path):
    with pytest.raises(ValueError, match="need baseline_log_source"):
        spark_forensics_callback(
            log_source=MagicMock(), backend=MagicMock(), report_dest=str(tmp_path / "r.json"), max_regression_pct=20,
        )


def test_callback_factory_rejects_a_regression_metric_without_max_regression_pct(tmp_path):
    with pytest.raises(ValueError, match="regression_metric needs max_regression_pct"):
        spark_forensics_callback(
            log_source=MagicMock(), backend=MagicMock(), report_dest=str(tmp_path / "r.json"),
            baseline_log_source=MagicMock(), regression_metric="gcTime",
        )
