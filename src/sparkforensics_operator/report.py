"""
Parses what the sparkforensics-analyze CLI produces: the JSON --out file
(findings/recommendations/cleanChecks/summary) and the stderr threshold
lines (the CLI's own budget evaluation isn't in the JSON output at all: see the "Global constraints" section of the implementation plan). Threshold
logic lives entirely upstream in sparkforensics; this module only parses
its output, so it can never drift from what the CLI actually enforces.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

THRESHOLD_CLI_FLAGS = {
    "max_runtime_ms": "--max-runtime",
    "max_spill_gb": "--max-spill",
    "max_skew_ratio": "--max-skew",
    "max_failed_task_rate_pct": "--max-failed-task-rate",
    "min_efficiency_pct": "--min-efficiency",
}

# CLI's own BudgetResult['name'] values (src/cli/budgets.ts), keyed by our
# thresholds dict's keys so parse_threshold_results can seed a "pass"
# default for every threshold the caller actually asked for.
THRESHOLD_RESULT_NAMES = {
    "max_runtime_ms": "max-runtime",
    "max_spill_gb": "max-spill",
    "max_skew_ratio": "max-skew",
    "max_failed_task_rate_pct": "max-failed-task-rate",
    "min_efficiency_pct": "min-efficiency",
}

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
    threshold_results: list[ThresholdResult] = field(default_factory=list)
    exit_code: int = 0

    @property
    def violated(self) -> bool:
        return any(r.status == "violation" for r in self.threshold_results)

    @property
    def inconclusive(self) -> bool:
        return any(r.status == "inconclusive" for r in self.threshold_results)


def parse_report_json(path: Path) -> Report:
    return parse_report_text(Path(path).read_text())


def parse_report_text(text: str) -> Report:
    """Parses the CLI's JSON report, as written to its --out file or, with
    no --out, to stdout."""
    data = json.loads(text)
    return Report(
        schema_version=data["schemaVersion"],
        summary=data["summary"],
        findings=data["findings"],
        recommendations=data["recommendations"],
        clean_checks=data["cleanChecks"],
        evidence_availability=data.get("evidenceAvailability"),
        detectors=data.get("detectors", []),
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
