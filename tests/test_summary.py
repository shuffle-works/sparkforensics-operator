from unittest.mock import MagicMock

import pytest

from sparkforensics_operator import spark_forensics_callback
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.log_ref import LocalEventLog
from sparkforensics_operator.operator import SparkForensicsOperator
from sparkforensics_operator.report import Report, ThresholdResult
from sparkforensics_operator.summary import SUMMARY_XCOM_KEY, render_report_url, validate_report_url_template


def _report(*threshold_results, exit_code=0, counts=None):
    return Report(
        schema_version=3,
        summary={"impactBandCounts": counts or {"critical": 2, "warning": 1}},
        findings=[], recommendations=[], clean_checks=[],
        threshold_results=list(threshold_results), exit_code=exit_code,
    )


def _run(tmp_path, report, **kwargs):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = report
    op = SparkForensicsOperator(
        task_id="forensics", log_source=log_source, backend=backend,
        report_dest=str(tmp_path / "report.json"), **kwargs,
    )
    ti = MagicMock()
    order = []
    ti.xcom_push.side_effect = lambda key, value: order.append(("xcom", key))
    notifier = MagicMock()
    notifier.notify.side_effect = lambda *a: order.append(("notify", None))
    op.notifier = notifier
    try:
        op.execute({"ti": ti})
        error = None
    except ThresholdBreached as e:
        error = e
    pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}
    return pushed.get(SUMMARY_XCOM_KEY), error, order


def test_the_summary_holds_the_verdict_and_leaves_return_value_to_the_destination(tmp_path):
    summary, error, _ = _run(tmp_path, _report(ThresholdResult("max-spill", "pass", "")), max_spill_gb=2)

    assert error is None
    assert summary == {
        "schema_version": 3,
        "destination": str(tmp_path / "report.json"),
        "report_url": None,
        "impact_band_counts": {"critical": 2, "warning": 1, "info": 0},
        "violated": False,
        "breached_thresholds": [],
        "inconclusive_thresholds": [],
        "exit_code": 0,
    }


def test_the_summary_is_pushed_before_notifying_and_before_a_breach_raises(tmp_path):
    report = _report(
        ThresholdResult("max-spill", "violation", "3 GB > 2 GB"),
        ThresholdResult("min-efficiency", "inconclusive", "no metrics"),
        exit_code=1,
    )

    summary, error, order = _run(tmp_path, report, max_spill_gb=2)

    assert isinstance(error, ThresholdBreached)
    assert order == [("xcom", SUMMARY_XCOM_KEY), ("notify", None)]
    assert summary["violated"] is True
    assert summary["breached_thresholds"] == ["max-spill"]
    assert summary["inconclusive_thresholds"] == ["min-efficiency"]


def test_an_exit_code_1_without_parsed_violations_still_reads_as_violated(tmp_path):
    summary, _, _ = _run(tmp_path, _report(exit_code=1), on_threshold_breach="warn")

    assert summary["violated"] is True
    assert summary["breached_thresholds"] == []


def test_the_summary_carries_the_report_url(tmp_path):
    summary, _, _ = _run(tmp_path, _report(), report_url_template="https://viewer.example/?report={path}")

    assert summary["report_url"] == f"https://viewer.example/?report={tmp_path}/report.json"


def test_no_summary_is_pushed_without_a_task_instance(tmp_path):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    backend = MagicMock()
    backend.analyze.return_value = _report()
    op = SparkForensicsOperator(task_id="f", log_source=log_source, backend=backend, report_dest=str(tmp_path / "r.json"))

    assert op.execute({}) == str(tmp_path / "r.json")


@pytest.mark.parametrize(
    "destination, template, url",
    [
        (
            "s3://reports/run 1/app.json",
            "https://s3.console.aws.amazon.com/s3/object/{bucket}?prefix={key}",
            "https://s3.console.aws.amazon.com/s3/object/reports?prefix=run%201/app.json",
        ),
        ("/mnt/reports/app.json", "https://viewer.example/view{path}", "https://viewer.example/view/mnt/reports/app.json"),
        ("file:///mnt/reports/app.json", "https://viewer.example/view{path}", "https://viewer.example/view/mnt/reports/app.json"),
        ("s3://b/k.json", "https://viewer.example/?d={destination}", "https://viewer.example/?d=s3%3A//b/k.json"),
    ],
    ids=["s3-console", "local-path", "file-uri", "destination"],
)
def test_render_report_url_fills_the_template_from_the_destination(destination, template, url):
    assert render_report_url(destination, template) == url


def test_render_report_url_is_none_without_a_template_or_destination():
    assert render_report_url("s3://b/k", None) is None
    assert render_report_url(None, "https://x/{path}") is None


@pytest.mark.parametrize(
    "template, message",
    [
        ("https://x/{nope}", "not a valid template"),
        ("javascript:alert('{path}')", "http:// or https://"),
        ("/relative/{path}", "http:// or https://"),
    ],
)
def test_the_operator_rejects_a_report_url_template_that_cannot_build_an_http_url(template, message):
    with pytest.raises(ValueError, match=message):
        validate_report_url_template(template)
    with pytest.raises(ValueError, match=message):
        SparkForensicsOperator(
            task_id="f", log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json",
            report_url_template=template,
        )
    with pytest.raises(ValueError, match=message):
        spark_forensics_callback(
            log_source=MagicMock(), backend=MagicMock(), report_dest="/tmp/r.json", report_url_template=template,
        )


def _compared(comparison):
    report = _report()
    report.comparison = comparison
    return report


def _metric(key, baseline, candidate, direction):
    delta = None if baseline is None or candidate is None else candidate - baseline
    return {"key": key, "baseline": baseline, "candidate": candidate, "delta": delta, "direction": direction}


def _run_compared(tmp_path, comparison, **kwargs):
    log_source = MagicMock()
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
    baseline_log_source = MagicMock()
    baseline_log_source.locate.return_value = LocalEventLog(tmp_path / "baseline.log")
    backend = MagicMock()
    backend.analyze.return_value = _compared(comparison)
    op = SparkForensicsOperator(
        task_id="forensics", log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        baseline_log_source=baseline_log_source, **kwargs,
    )
    ti = MagicMock()
    op.execute({"ti": ti})
    return {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}[SUMMARY_XCOM_KEY]


def test_a_baseline_run_adds_the_comparison_on_the_default_metric_and_keeps_every_other_key(tmp_path):
    comparison = {
        "confidence": "low", "reason": "Run names differ, so deltas may compare different work.",
        "metrics": [_metric("wallClock", 1000, 1250, "regression"), _metric("gcTime", 10, 5, "improvement")],
    }

    summary = _run_compared(tmp_path, comparison)

    assert summary.pop("comparison") == {
        "verdict": "regression",
        "confidence": "low",
        "reason": "Run names differ, so deltas may compare different work.",
        "metric": "wallClock",
        "baseline": 1000,
        "candidate": 1250,
        "regression_pct": 25.0,
    }
    assert summary == {
        "schema_version": 3,
        "destination": str(tmp_path / "report.json"),
        "report_url": None,
        "impact_band_counts": {"critical": 2, "warning": 1, "info": 0},
        "violated": False,
        "breached_thresholds": [],
        "inconclusive_thresholds": [],
        "exit_code": 0,
    }


def test_the_comparison_reports_on_the_configured_regression_metric(tmp_path):
    comparison = {
        "confidence": "ok", "reason": None,
        "metrics": [_metric("wallClock", 1000, 1250, "regression"), _metric("gcTime", 10, 5, "improvement")],
    }

    summary = _run_compared(tmp_path, comparison, max_regression_pct=20, regression_metric="gcTime")

    assert summary["comparison"]["metric"] == "gcTime"
    assert summary["comparison"]["verdict"] == "improvement"
    assert summary["comparison"]["regression_pct"] == -50.0


@pytest.mark.parametrize("metrics, verdict", [
    ([], "unavailable"),
    ([_metric("wallClock", None, 1250, "unavailable")], "unavailable"),
    ([_metric("wallClock", 0, 1250, "regression")], "regression"),
], ids=["missing", "unmeasured", "zero-baseline"])
def test_the_comparison_has_no_regression_pct_when_it_has_no_finite_value(tmp_path, metrics, verdict):
    summary = _run_compared(tmp_path, {"confidence": "ok", "reason": None, "metrics": metrics})

    assert summary["comparison"]["verdict"] == verdict
    assert summary["comparison"]["regression_pct"] is None
