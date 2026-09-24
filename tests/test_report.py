import json

from sparkforensics_operator.report import (
    THRESHOLD_CLI_FLAGS,
    Report,
    ThresholdResult,
    parse_report_text,
    parse_threshold_results,
)

SAMPLE_JSON = {
    "schemaVersion": 3,
    "summary": {
        "app": {"id": "app-1", "name": "etl_ar_ventas", "sparkVersion": "3.5.1"},
        "stageCount": 12,
        "jobCount": 4,
        "sqlExecutionCount": 2,
        "findingCount": 3,
        "impactBandCounts": {"critical": 1, "warning": 1, "info": 1},
    },
    "evidenceAvailability": None,
    "detectors": [],
    "findings": [{"id": "f1", "type": "spill", "tag": "spill", "impactBand": "critical"}],
    "recommendations": [],
    "cleanChecks": [],
}


def test_parse_report_text_reads_the_cli_json_report():
    report = parse_report_text(json.dumps(SAMPLE_JSON))

    assert report.schema_version == 3
    assert report.summary["impactBandCounts"]["critical"] == 1
    assert report.findings[0]["type"] == "spill"
    assert report.threshold_results == []


def test_parse_report_text_captures_evidence_availability_and_detectors():
    data = dict(SAMPLE_JSON, evidenceAvailability={"taskLevel": False}, detectors=["spill", "skew"])

    report = parse_report_text(json.dumps(data))

    assert report.evidence_availability == {"taskLevel": False}
    assert report.detectors == ["spill", "skew"]


def test_parse_report_text_defaults_evidence_availability_and_detectors_when_absent():
    data = {k: v for k, v in SAMPLE_JSON.items() if k not in ("evidenceAvailability", "detectors")}

    report = parse_report_text(json.dumps(data))

    assert report.evidence_availability is None
    assert report.detectors == []


def test_threshold_cli_flags_cover_every_thresholds_key():
    assert set(THRESHOLD_CLI_FLAGS) == {
        "max_runtime_ms",
        "max_spill_gb",
        "max_skew_ratio",
        "max_failed_task_rate_pct",
        "min_efficiency_pct",
    }
    assert THRESHOLD_CLI_FLAGS["max_runtime_ms"] == "--max-runtime"
    assert THRESHOLD_CLI_FLAGS["max_spill_gb"] == "--max-spill"
    assert THRESHOLD_CLI_FLAGS["max_skew_ratio"] == "--max-skew"
    assert THRESHOLD_CLI_FLAGS["max_failed_task_rate_pct"] == "--max-failed-task-rate"
    assert THRESHOLD_CLI_FLAGS["min_efficiency_pct"] == "--min-efficiency"


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
        schema_version=3, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-runtime", "pass", "")],
    )
    assert not passing.violated
    assert not passing.inconclusive

    violated = Report(
        schema_version=3, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-runtime", "violation", "too slow")],
    )
    assert violated.violated
    assert not violated.inconclusive

    inconclusive = Report(
        schema_version=3, summary={}, findings=[], recommendations=[], clean_checks=[],
        threshold_results=[ThresholdResult("max-skew", "inconclusive", "no data")],
    )
    assert not inconclusive.violated
    assert inconclusive.inconclusive
