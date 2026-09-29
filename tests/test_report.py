import json
from pathlib import Path

import pytest

from sparkforensics_operator.report import (
    THRESHOLD_CLI_FLAGS,
    Report,
    ThresholdResult,
    parse_report_text,
    parse_threshold_results,
)

# Real sparkforensics-cli 0.4.0 output (--redact) for public corpus logs; see
# tests/fixtures/sparkforensics_cli_0_4_0/README.md for how it was produced.
CLI_FIXTURES = Path(__file__).parent / "fixtures" / "sparkforensics_cli_0_4_0"


def _fixture_text(name: str) -> str:
    return (CLI_FIXTURES / name).read_text()


def test_parse_report_text_reads_a_real_cli_report():
    data = json.loads(_fixture_text("report.json"))

    report = parse_report_text(_fixture_text("report.json"))

    assert report.schema_version == 5
    assert report.summary == data["summary"]
    assert report.verdict == data["verdict"]
    assert report.evidence_availability == data["evidenceAvailability"]
    assert report.detectors == data["detectors"]
    assert report.findings == data["findings"]
    assert report.recommendations == data["recommendations"]
    assert report.clean_checks == data["cleanChecks"]
    assert report.not_run_checks == data["notRunChecks"]
    assert report.comparison is None
    assert report.threshold_results == []


def test_a_real_cli_report_carries_text_figures_in_value_text():
    findings = parse_report_text(_fixture_text("report.json")).findings

    assert all(f["value"] is None or isinstance(f["value"], (int, float)) for f in findings)
    text_findings = [f for f in findings if "valueText" in f]
    assert {f["type"] for f in text_findings} >= {"stageFailed", "configAudit"}
    assert all(f["value"] is None for f in text_findings)


def test_parse_report_text_unwraps_the_candidate_report_of_a_real_baseline_run():
    data = json.loads(_fixture_text("comparison.json"))

    report = parse_report_text(_fixture_text("comparison.json"))

    assert report.schema_version == 5
    assert report.findings == data["candidate"]["findings"]
    assert report.not_run_checks == data["candidate"]["notRunChecks"]
    assert report.comparison == data["comparison"]


def test_parse_threshold_results_reads_real_cli_stderr():
    thresholds = {"max_runtime_ms": 1, "max_skew_ratio": 2, "max_spill_gb": 100}

    results = {r.name: r for r in parse_threshold_results(thresholds, _fixture_text("report.stderr"))}

    assert results["max-runtime"].status == "violation"
    assert results["max-skew"].status == "violation"
    assert results["max-spill"].status == "pass"


def test_parse_threshold_results_reads_a_real_incomplete_run():
    results = parse_threshold_results({}, _fixture_text("incomplete-run.stderr"))

    assert [(r.name, r.status) for r in results] == [("run-complete", "inconclusive")]
    report = parse_report_text(_fixture_text("incomplete-run.json"))
    assert report.schema_version == 5
    assert {c["type"] for c in report.not_run_checks} == {
        c["type"] for c in json.loads(_fixture_text("incomplete-run.json"))["notRunChecks"]
    }
    assert report.not_run_checks


@pytest.mark.parametrize("schema_version", [3, 4])
def test_parse_report_text_refuses_a_report_older_than_sparkforensics_cli_0_4_0(schema_version):
    data = dict(json.loads(_fixture_text("report.json")), schemaVersion=schema_version)

    with pytest.raises(ValueError, match="requires sparkforensics-cli 0.4.0 or newer"):
        parse_report_text(json.dumps(data))


def test_threshold_cli_flags_cover_every_thresholds_key():
    assert set(THRESHOLD_CLI_FLAGS) == {
        "max_runtime_ms",
        "max_spill_gb",
        "max_skew_ratio",
        "max_failed_task_rate_pct",
        "min_efficiency_pct",
        "max_regression_pct",
        "regression_metric",
        "fail_on_introduced",
    }
    assert THRESHOLD_CLI_FLAGS["max_runtime_ms"] == "--max-runtime"
    assert THRESHOLD_CLI_FLAGS["max_spill_gb"] == "--max-spill"
    assert THRESHOLD_CLI_FLAGS["max_skew_ratio"] == "--max-skew"
    assert THRESHOLD_CLI_FLAGS["max_failed_task_rate_pct"] == "--max-failed-task-rate"
    assert THRESHOLD_CLI_FLAGS["min_efficiency_pct"] == "--min-efficiency"
    assert THRESHOLD_CLI_FLAGS["max_regression_pct"] == "--max-regression-pct"
    assert THRESHOLD_CLI_FLAGS["regression_metric"] == "--regression-metric"
    assert THRESHOLD_CLI_FLAGS["fail_on_introduced"] == "--fail-on-introduced"


def test_parse_threshold_results_defaults_requested_thresholds_to_pass():
    thresholds = {"max_runtime_ms": 10_000, "max_skew_ratio": 3.0}
    stderr = ""

    results = parse_threshold_results(thresholds, stderr)

    assert sorted(r.name for r in results) == ["max-runtime", "max-skew"]
    assert all(r.status == "pass" for r in results)


def test_parse_threshold_results_reads_violation_and_inconclusive_lines():
    thresholds = {"max_runtime_ms": 10_000, "max_skew_ratio": 3.0, "min_efficiency_pct": 50}
    stderr = (
        "[violation] max-runtime: Runtime 12000ms exceeds budget 10000ms.\n"
        "[inconclusive] max-skew: No trustworthy task-level evidence to measure skew.\n"
    )

    results = {r.name: r for r in parse_threshold_results(thresholds, stderr)}

    assert results["max-runtime"] == ThresholdResult(
        name="max-runtime", status="violation",
        detail="Runtime 12000ms exceeds budget 10000ms.",
    )
    assert results["max-skew"].status == "inconclusive"
    assert results["min-efficiency"].status == "pass"


def test_parse_threshold_results_always_surfaces_run_complete_even_if_unrequested():
    thresholds = {"max_runtime_ms": 10_000}
    stderr = "[inconclusive] run-complete: App never recorded an ApplicationEnd event.\n"

    results = {r.name: r for r in parse_threshold_results(thresholds, stderr)}

    assert results["run-complete"].status == "inconclusive"
    assert results["max-runtime"].status == "pass"


def test_report_violated_and_inconclusive_properties():
    passing = Report(
        schema_version=5, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-runtime", "pass", "")],
    )
    assert not passing.violated
    assert not passing.inconclusive

    violated = Report(
        schema_version=5, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-runtime", "violation", "too slow")],
    )
    assert violated.violated
    assert not violated.inconclusive

    inconclusive = Report(
        schema_version=5, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-skew", "inconclusive", "no data")],
    )
    assert not inconclusive.violated
    assert inconclusive.inconclusive


def test_parse_threshold_results_seeds_the_comparison_budgets_but_not_the_metric():
    thresholds = {"max_regression_pct": 20, "regression_metric": "gcTime", "fail_on_introduced": "critical"}
    stderr = '[violation] max-regression: Metric "gcTime" regressed 31.0%, exceeding budget 20%.\n'

    results = {r.name: r for r in parse_threshold_results(thresholds, stderr)}

    assert set(results) == {"max-regression", "fail-on-introduced"}
    assert results["max-regression"].status == "violation"
    assert results["fail-on-introduced"].status == "pass"
