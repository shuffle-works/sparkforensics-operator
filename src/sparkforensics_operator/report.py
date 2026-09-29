"""
Parses what the sparkforensics-analyze CLI produces: the JSON --out file
(summary/verdict/findings/recommendations/cleanChecks/notRunChecks) and the stderr threshold
lines (the CLI's own budget evaluation isn't in the JSON output at all: see the "Global constraints" section of the implementation plan). Threshold
logic lives entirely upstream in sparkforensics; this module only parses
its output, so it can never drift from what the CLI actually enforces.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

THRESHOLD_CLI_FLAGS = {
    "max_runtime_ms": "--max-runtime",
    "max_spill_gb": "--max-spill",
    "max_skew_ratio": "--max-skew",
    "max_failed_task_rate_pct": "--max-failed-task-rate",
    "min_efficiency_pct": "--min-efficiency",
    # The comparison budgets: the CLI accepts them only with --baseline, and
    # --regression-metric only with --max-regression-pct.
    "max_regression_pct": "--max-regression-pct",
    "regression_metric": "--regression-metric",
    "fail_on_introduced": "--fail-on-introduced",
}

# The metric --max-regression-pct checks when --regression-metric is unset.
DEFAULT_REGRESSION_METRIC = "wallClock"

# CLI's own BudgetResult['name'] values (src/cli/budgets.ts), keyed by our
# thresholds dict's keys so parse_threshold_results can seed a "pass"
# default for every threshold the caller actually asked for.
THRESHOLD_RESULT_NAMES = {
    "max_runtime_ms": "max-runtime",
    "max_spill_gb": "max-spill",
    "max_skew_ratio": "max-skew",
    "max_failed_task_rate_pct": "max-failed-task-rate",
    "min_efficiency_pct": "min-efficiency",
    "max_regression_pct": "max-regression",
    "fail_on_introduced": "fail-on-introduced",
}

# The report schema sparkforensics-cli 0.4.0 introduced (each finding's text
# figure moved from value to valueText). An older report is refused rather
# than parsed into a partial one.
MIN_SCHEMA_VERSION = 5

_THRESHOLD_LINE_RE = re.compile(r"^\[(violation|inconclusive)\] ([a-z-]+): (.*)$")


@dataclass(frozen=True)
class ThresholdResult:
    name: str
    status: str  # "pass" | "violation" | "inconclusive"
    detail: str


@dataclass
class Report:
    schema_version: int
    summary: dict
    findings: list
    recommendations: list
    clean_checks: list
    evidence_availability: dict | None = None
    detectors: list = field(default_factory=list)
    # The CLI's run verdict (title, summary, next steps) and the checks the
    # log lacked the data to run.
    verdict: dict | None = None
    not_run_checks: list = field(default_factory=list)
    threshold_results: list[ThresholdResult] = field(default_factory=list)
    exit_code: int = 0
    # The CLI's comparison section (confidence, reason, matchedCoverage,
    # metrics, findings), present only when it ran with --baseline.
    comparison: dict | None = None

    @property
    def violated(self) -> bool:
        return any(r.status == "violation" for r in self.threshold_results)

    @property
    def inconclusive(self) -> bool:
        return any(r.status == "inconclusive" for r in self.threshold_results)


def parse_report_text(text: str) -> Report:
    """Parses the CLI's JSON report, as written to its --out file or, with
    no --out, to stdout. With --baseline the CLI wraps the run's own report
    as {"candidate": <report>, "comparison": {...}}."""
    data = json.loads(text)
    comparison = None
    if isinstance(data, dict) and "candidate" in data and "comparison" in data:
        comparison = data["comparison"]
        data = data["candidate"]
    schema_version = data["schemaVersion"]
    if not isinstance(schema_version, int) or schema_version < MIN_SCHEMA_VERSION:
        raise ValueError(
            f"report schemaVersion {schema_version!r} is not supported (need "
            f"{MIN_SCHEMA_VERSION} or newer): sparkforensics-operator requires "
            "sparkforensics-cli 0.4.0 or newer."
        )
    return Report(
        schema_version=schema_version,
        summary=data["summary"],
        findings=data["findings"],
        recommendations=data["recommendations"],
        clean_checks=data["cleanChecks"],
        evidence_availability=data["evidenceAvailability"],
        detectors=data["detectors"],
        verdict=data["verdict"],
        not_run_checks=data["notRunChecks"],
        comparison=comparison,
    )


def parse_threshold_results(thresholds: dict, stderr: str) -> list[ThresholdResult]:
    requested_names = {
        THRESHOLD_RESULT_NAMES[key]
        for key, value in thresholds.items()
        if key in THRESHOLD_RESULT_NAMES and value is not None
    }
    results = {name: ThresholdResult(name=name, status="pass", detail="") for name in requested_names}
    for line in stderr.splitlines():
        match = _THRESHOLD_LINE_RE.match(line)
        if not match:
            continue
        status, name, detail = match.groups()
        results[name] = ThresholdResult(name=name, status=status, detail=detail)
    return list(results.values())
