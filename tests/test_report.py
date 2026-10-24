import json
from pathlib import Path

import pytest

from sparkforensics_operator.report import (
    MAX_SCHEMA_VERSION,
    TESTED_CLI_VERSION,
    THRESHOLD_CLI_FLAGS,
    THRESHOLD_CLI_REPEATED_FLAGS,
    THRESHOLD_CLI_SWITCHES,
    Report,
    ThresholdResult,
    UnsupportedReportSchema,
    cli_version_warning,
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


@pytest.mark.parametrize("schema_version", [MAX_SCHEMA_VERSION + 1, MAX_SCHEMA_VERSION + 10])
def test_parse_report_text_refuses_a_report_newer_than_the_schema_it_reads(schema_version):
    data = dict(json.loads(_fixture_text("report.json")), schemaVersion=schema_version)

    with pytest.raises(UnsupportedReportSchema) as raised:
        parse_report_text(json.dumps(data))

    message = str(raised.value)
    assert f"schemaVersion {schema_version} is newer than this sparkforensics-operator reads" in message
    assert f"up to {MAX_SCHEMA_VERSION}" in message
    assert "upgrade sparkforensics-operator" in message


def test_an_unsupported_schema_is_a_value_error():
    assert issubclass(UnsupportedReportSchema, ValueError)


def test_parse_report_text_reads_the_cli_version_from_the_generator():
    data = dict(
        json.loads(_fixture_text("report.json")),
        generator={"name": "sparkforensics-cli", "version": "0.10.1", "buildId": "abc"},
    )

    assert parse_report_text(json.dumps(data)).cli_version == "0.10.1"


def test_parse_report_text_has_no_cli_version_without_a_generator():
    assert parse_report_text(_fixture_text("report.json")).cli_version is None


@pytest.mark.parametrize("cli_version", [TESTED_CLI_VERSION, "0.10.2", "0.11.0", "1.0.0", "0.10.2-rc.1"])
def test_no_warning_for_the_tested_cli_version_or_a_newer_one(cli_version):
    assert cli_version_warning(cli_version) is None


@pytest.mark.parametrize("cli_version", ["0.10.0", "0.9.0", "0.8.0", "0.4.0"])
def test_a_cli_older_than_the_tested_version_gets_a_warning(cli_version):
    warning = cli_version_warning(cli_version)

    assert f"sparkforensics-cli {cli_version} is older than {TESTED_CLI_VERSION}" in warning
    assert "npm install -g sparkforensics-cli" in warning


def test_a_report_without_a_cli_version_gets_a_warning():
    warning = cli_version_warning(None)

    assert "does not name its sparkforensics-cli" in warning
    assert "0.8.0" in warning


def test_report_reads_the_verdict_app_run_time_and_skipped_lines_from_the_cli_summary():
    data = json.loads(_fixture_text("report.json"))
    data["summary"]["skippedLines"] = 4

    report = parse_report_text(json.dumps(data))

    assert report.verdict_title == data["verdict"]["title"]
    assert report.app_name == data["summary"]["app"]["name"]
    assert report.run_time_ms == data["summary"]["runShape"]["wallClockMs"]
    assert report.skipped_lines == 4


def test_report_has_no_run_time_for_a_run_that_never_finished():
    report = parse_report_text(_fixture_text("incomplete-run.json"))

    assert report.run_time_ms is None
    assert report.skipped_lines is None


def test_report_properties_are_none_when_the_summary_lacks_the_fields():
    report = Report(schema_version=5, summary={}, findings=[], recommendations=[], clean_checks=[])

    assert report.app_name is None
    assert report.run_time_ms is None
    assert report.skipped_lines is None
    assert report.verdict_title is None


def test_parse_report_text_keeps_the_whole_cli_report_in_raw():
    data = dict(json.loads(_fixture_text("report.json")), metrics={"schemaVersion": 1}, extra="kept")

    report = parse_report_text(json.dumps(data))

    assert report.raw == data


def test_parse_report_text_keeps_the_candidate_report_in_raw_when_comparing():
    wrapped = json.loads(_fixture_text("comparison.json"))

    report = parse_report_text(json.dumps(wrapped))

    assert report.raw == wrapped["candidate"]
    assert report.comparison == wrapped["comparison"]


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
        "stage_qualities",
        "noise_band_pct",
    }
    assert THRESHOLD_CLI_FLAGS["max_runtime_ms"] == "--max-runtime"
    assert THRESHOLD_CLI_FLAGS["max_spill_gb"] == "--max-spill"
    assert THRESHOLD_CLI_FLAGS["max_skew_ratio"] == "--max-skew"
    assert THRESHOLD_CLI_FLAGS["max_failed_task_rate_pct"] == "--max-failed-task-rate"
    assert THRESHOLD_CLI_FLAGS["min_efficiency_pct"] == "--min-efficiency"
    assert THRESHOLD_CLI_FLAGS["max_regression_pct"] == "--max-regression-pct"
    assert THRESHOLD_CLI_FLAGS["regression_metric"] == "--regression-metric"
    assert THRESHOLD_CLI_FLAGS["fail_on_introduced"] == "--fail-on-introduced"
    assert THRESHOLD_CLI_FLAGS["stage_qualities"] == "--stage-quality"
    assert THRESHOLD_CLI_FLAGS["noise_band_pct"] == "--noise-band-pct"


def test_threshold_cli_repeated_flags_are_the_per_metric_budgets_and_the_comparison_patterns():
    assert THRESHOLD_CLI_REPEATED_FLAGS == {
        "regression_budgets": "--regression-budget",
        "stage_regression_budgets": "--stage-regression-budget",
        "normalize_path": "--normalize-path",
        "ignore_conf": "--ignore-conf",
    }


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


def test_parse_threshold_results_seeds_a_pass_for_each_kind_of_regression_budget():
    thresholds = {
        "regression_budgets": {"gcTime": 10, "wallClock": 20},
        "stage_regression_budgets": {"executorRunTime": 30},
    }

    results = parse_threshold_results(thresholds, "")

    assert [(r.name, r.status) for r in results] == [("max-regression", "pass"), ("max-stage-regression", "pass")]


def test_parse_threshold_results_keeps_every_violated_regression_budget():
    thresholds = {"max_regression_pct": 20, "regression_budgets": {"gcTime": 10, "diskSpill": 5}}
    stderr = (
        '[violation] max-regression: Metric "wallClock" regressed 31.0%, exceeding budget 20%.\n'
        '[violation] max-regression: Metric "gcTime" regressed 12.0%, exceeding budget 10%.\n'
        '[inconclusive] max-regression: Metric "diskSpill" is unavailable for this comparison.\n'
    )

    results = parse_threshold_results(thresholds, stderr)

    assert [(r.name, r.status) for r in results] == [
        ("max-regression", "violation"), ("max-regression", "violation"), ("max-regression", "inconclusive"),
    ]
    assert 'Metric "gcTime"' in results[1].detail


def test_parse_threshold_results_replaces_the_seeded_pass_only_for_the_names_printed():
    thresholds = {"regression_budgets": {"gcTime": 10}, "stage_regression_budgets": {"executorRunTime": 30}}
    stderr = '[violation] max-stage-regression: 2 of 5 paired stage(s) regressed "executorRunTime" beyond 30%.\n'

    results = parse_threshold_results(thresholds, stderr)

    assert [(r.name, r.status) for r in results] == [
        ("max-regression", "pass"), ("max-stage-regression", "violation"),
    ]


def test_threshold_cli_switches_are_redact():
    assert THRESHOLD_CLI_SWITCHES == {"redact": "--redact"}


def test_redact_adds_no_threshold_result():
    assert parse_threshold_results({"redact": True}, "") == []


def test_comparison_tuning_adds_no_threshold_result():
    thresholds = {"normalize_path": ["/out/\\d+"], "ignore_conf": ["^spark\\.app\\."], "noise_band_pct": 5}

    assert parse_threshold_results(thresholds, "") == []


def test_parse_threshold_results_ignores_an_empty_budget_dict():
    assert parse_threshold_results({"regression_budgets": {}, "stage_regression_budgets": None}, "") == []


def test_parse_threshold_results_lists_the_requested_names_in_the_order_given():
    thresholds = {"min_efficiency_pct": 50, "max_runtime_ms": 1, "max_skew_ratio": 2, "max_spill_gb": 3}

    assert [r.name for r in parse_threshold_results(thresholds, "")] == [
        "min-efficiency", "max-runtime", "max-skew", "max-spill",
    ]


def test_parse_threshold_results_seeds_the_comparison_budgets_but_not_the_metric():
    thresholds = {"max_regression_pct": 20, "regression_metric": "gcTime", "fail_on_introduced": "critical"}
    stderr = '[violation] max-regression: Metric "gcTime" regressed 31.0%, exceeding budget 20%.\n'

    results = {r.name: r for r in parse_threshold_results(thresholds, stderr)}

    assert set(results) == {"max-regression", "fail-on-introduced"}
    assert results["max-regression"].status == "violation"
    assert results["fail-on-introduced"].status == "pass"
