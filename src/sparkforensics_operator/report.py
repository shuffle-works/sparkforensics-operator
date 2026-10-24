"""
Parses what the sparkforensics-analyze CLI produces: the JSON --out file
(summary/verdict/findings/recommendations/cleanChecks/notRunChecks) and the
stderr threshold lines (the CLI's own budget evaluation isn't in the JSON output at all: see the "Global constraints" section of the implementation plan). Threshold
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
    # The comparison budgets need --baseline, and --regression-metric needs
    # --max-regression-pct.
    "max_regression_pct": "--max-regression-pct",
    "regression_metric": "--regression-metric",
    "fail_on_introduced": "--fail-on-introduced",
    "stage_qualities": "--stage-quality",  # a list, joined with commas
    "noise_band_pct": "--noise-band-pct",
}

THRESHOLD_CLI_SWITCHES = {"redact": "--redact"}

# Flags given once per item: a list item, or a {metric: percentage} entry as
# "<metric>:<percentage>".
THRESHOLD_CLI_REPEATED_FLAGS = {
    "regression_budgets": "--regression-budget",
    "stage_regression_budgets": "--stage-regression-budget",
    "normalize_path": "--normalize-path",
    "ignore_conf": "--ignore-conf",
}

# The metric --max-regression-pct checks when --regression-metric is unset.
DEFAULT_REGRESSION_METRIC = "wallClock"

# The CLI's BudgetResult names, keyed by thresholds key, so that
# parse_threshold_results can seed a "pass" for each requested threshold.
THRESHOLD_RESULT_NAMES = {
    "max_runtime_ms": "max-runtime",
    "max_spill_gb": "max-spill",
    "max_skew_ratio": "max-skew",
    "max_failed_task_rate_pct": "max-failed-task-rate",
    "min_efficiency_pct": "min-efficiency",
    "max_regression_pct": "max-regression",
    "regression_budgets": "max-regression",
    "stage_regression_budgets": "max-stage-regression",
    "fail_on_introduced": "fail-on-introduced",
}

# The report schemas the operator reads. The CLI raises schemaVersion only for
# a change that can break a reader.
MIN_SCHEMA_VERSION = 5
MAX_SCHEMA_VERSION = 5

TESTED_CLI_VERSION = "0.10.1"

_THRESHOLD_LINE_RE = re.compile(r"^\[(violation|inconclusive)\] ([a-z-]+): (.*)$")


class UnsupportedReportSchema(ValueError):
    pass


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
    verdict: dict | None = None
    not_run_checks: list = field(default_factory=list)
    threshold_results: list[ThresholdResult] = field(default_factory=list)
    exit_code: int = 0
    comparison: dict | None = None
    cli_version: str | None = None
    raw: dict | None = None
    markdown: str | None = None

    @property
    def verdict_title(self) -> str | None:
        return _nested(self.verdict, "title")

    @property
    def app_name(self) -> str | None:
        return _nested(self.summary, "app", "name")

    @property
    def run_time_ms(self) -> int | None:
        return _nested(self.summary, "runShape", "wallClockMs")

    @property
    def skipped_lines(self) -> int | None:
        """None for a CLI before 0.10.2, which does not count them."""
        return _nested(self.summary, "skippedLines")

    @property
    def violated(self) -> bool:
        return any(r.status == "violation" for r in self.threshold_results)

    @property
    def inconclusive(self) -> bool:
        return any(r.status == "inconclusive" for r in self.threshold_results)


def _nested(data, *keys):
    for key in keys:
        data = data.get(key) if isinstance(data, dict) else None
    return data


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
        raise UnsupportedReportSchema(
            f"report schemaVersion {schema_version!r} is not supported (need "
            f"{MIN_SCHEMA_VERSION} or newer): sparkforensics-operator requires "
            "sparkforensics-cli 0.4.0 or newer."
        )
    if schema_version > MAX_SCHEMA_VERSION:
        raise UnsupportedReportSchema(
            f"report schemaVersion {schema_version} is newer than this "
            f"sparkforensics-operator reads (up to {MAX_SCHEMA_VERSION}): upgrade "
            "sparkforensics-operator, or install a sparkforensics-cli release that "
            f"writes schema {MAX_SCHEMA_VERSION}."
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
        cli_version=_nested(data, "generator", "version"),
        raw=data,
    )


def cli_version_warning(cli_version: str | None) -> str | None:
    """A warning when the CLI is older than TESTED_CLI_VERSION. A report
    without a version is from a CLI before 0.8.0."""
    if cli_version is None:
        older = "The report does not name its sparkforensics-cli, so it is older than 0.8.0"
    elif _release(cli_version) < _release(TESTED_CLI_VERSION):
        older = f"sparkforensics-cli {cli_version} is older than {TESTED_CLI_VERSION}"
    else:
        return None
    return f"{older}, the release this operator is tested against. Upgrade it with `npm install -g sparkforensics-cli`."


def _release(version: str) -> tuple[int, ...]:
    """"0.10.1" or "0.10.2-rc.1" as (0, 10, 1) or (0, 10, 2)."""
    return tuple(int(part) for part in re.match(r"\d+(?:\.\d+)*", version).group().split("."))


def parse_threshold_results(thresholds: dict, stderr: str) -> list[ThresholdResult]:
    """A result per threshold line the CLI printed (it prints only violations
    and inconclusive results), and a pass for each requested threshold it
    printed none for. Regression budgets all print under one name, so a name
    can have several results."""
    matches = filter(None, map(_THRESHOLD_LINE_RE.match, stderr.splitlines()))
    printed = [ThresholdResult(name, status, detail) for status, name, detail in (m.groups() for m in matches)]
    printed_names = {result.name for result in printed}
    requested = dict.fromkeys(
        THRESHOLD_RESULT_NAMES[key]
        for key, value in thresholds.items()
        if key in THRESHOLD_RESULT_NAMES and value not in (None, {})
    )
    passes = [ThresholdResult(name, "pass", "") for name in requested if name not in printed_names]
    return passes + printed
