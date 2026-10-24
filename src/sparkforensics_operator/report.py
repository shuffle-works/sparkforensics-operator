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
    # The comparison budgets: the CLI accepts them only with --baseline, and
    # --regression-metric only with --max-regression-pct.
    "max_regression_pct": "--max-regression-pct",
    "regression_metric": "--regression-metric",
    "fail_on_introduced": "--fail-on-introduced",
    # A list of pair qualities, joined with commas.
    "stage_qualities": "--stage-quality",
}

# The flags the CLI takes once per value. A dict of {metric: percentage}
# becomes one "<flag> <metric>:<percentage>" each.
THRESHOLD_CLI_REPEATED_FLAGS = {
    "regression_budgets": "--regression-budget",
    "stage_regression_budgets": "--stage-regression-budget",
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
    "regression_budgets": "max-regression",
    "stage_regression_budgets": "max-stage-regression",
    "fail_on_introduced": "fail-on-introduced",
}

# The report schemas this operator reads. 5 is the schema sparkforensics-cli
# 0.4.0 introduced (each finding's text figure moved from value to
# valueText). An older report is refused rather than parsed into a partial
# one, and so is a newer one: the CLI raises schemaVersion on a change that
# can break a reader, and only additive changes keep it.
MIN_SCHEMA_VERSION = 5
MAX_SCHEMA_VERSION = 5

# The sparkforensics-cli release the operator is tested against: the
# integration tests run it. A run analyzed by an older one logs a warning.
TESTED_CLI_VERSION = "0.10.1"

_THRESHOLD_LINE_RE = re.compile(r"^\[(violation|inconclusive)\] ([a-z-]+): (.*)$")


class UnsupportedReportSchema(ValueError):
    """The report's schemaVersion is outside MIN_SCHEMA_VERSION to
    MAX_SCHEMA_VERSION."""


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
    # The version of the sparkforensics-cli that wrote the report
    # (generator.version), None when it does not say.
    cli_version: str | None = None
    # The CLI's whole report as it parsed (the run's own report, without the
    # comparison section), so what the operator does not read by name is
    # still persisted. None for a report built by hand.
    raw: dict | None = None

    @property
    def verdict_title(self) -> str | None:
        title = self.verdict.get("title") if isinstance(self.verdict, dict) else None
        return title if isinstance(title, str) else None

    @property
    def app_name(self) -> str | None:
        app = self.summary.get("app") if isinstance(self.summary, dict) else None
        name = app.get("name") if isinstance(app, dict) else None
        return name if isinstance(name, str) else None

    @property
    def run_time_ms(self) -> int | float | None:
        """The application's wall-clock time, None when the log does not
        say (a run that never finished, say)."""
        shape = self.summary.get("runShape") if isinstance(self.summary, dict) else None
        wall_clock = shape.get("wallClockMs") if isinstance(shape, dict) else None
        return wall_clock if isinstance(wall_clock, (int, float)) and not isinstance(wall_clock, bool) else None

    @property
    def skipped_lines(self) -> int | None:
        """How many event log lines the CLI could not parse, None when the
        CLI does not say (before 0.10.2)."""
        count = self.summary.get("skippedLines") if isinstance(self.summary, dict) else None
        return count if isinstance(count, int) and not isinstance(count, bool) else None

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
        cli_version=_generator_version(data),
        raw=data,
    )


def _generator_version(data: dict) -> str | None:
    generator = data.get("generator")
    version = generator.get("version") if isinstance(generator, dict) else None
    return version if isinstance(version, str) else None


def cli_version_warning(cli_version: str | None) -> str | None:
    """Why a run analyzed by this sparkforensics-cli version is worth a
    warning, or None. The CLI names itself in its report from 0.8.0 on, so
    a report without a version comes from an older CLI; one whose version
    cannot be read is not judged."""
    if cli_version is None:
        return (
            "The report does not name the sparkforensics-cli that wrote it, which the CLI "
            f"does from 0.8.0 on, and this sparkforensics-operator is tested against "
            f"{TESTED_CLI_VERSION}. Upgrade it with `npm install -g sparkforensics-cli`."
        )
    installed, tested = _release(cli_version), _release(TESTED_CLI_VERSION)
    if installed is None or installed >= tested:
        return None
    return (
        f"sparkforensics-cli {cli_version} is older than {TESTED_CLI_VERSION}, the release "
        "this sparkforensics-operator is tested against. Upgrade it with "
        "`npm install -g sparkforensics-cli`."
    )


def _release(version: str) -> tuple[int, ...] | None:
    """The numeric release of a version such as "0.10.1" or "0.10.2-rc.1"."""
    match = re.match(r"^v?(\d+(?:\.\d+)*)", version.strip())
    if match is None:
        return None
    parts = [int(part) for part in match.group(1).split(".")]
    return tuple((parts + [0, 0, 0])[:3])


def parse_threshold_results(thresholds: dict, stderr: str) -> list[ThresholdResult]:
    """One result per threshold line the CLI printed (only violations and
    inconclusive results are printed), and a pass for each requested
    threshold it printed none for. Several regression budgets all print as
    max-regression (or max-stage-regression), so a name can have several
    results, one per budget that did not pass."""
    requested_names = []
    for key, value in thresholds.items():
        name = THRESHOLD_RESULT_NAMES.get(key)
        if name is not None and value is not None and value != {} and name not in requested_names:
            requested_names.append(name)
    results = {name: [ThresholdResult(name=name, status="pass", detail="")] for name in requested_names}
    printed: set[str] = set()
    for line in stderr.splitlines():
        match = _THRESHOLD_LINE_RE.match(line)
        if not match:
            continue
        status, name, detail = match.groups()
        if name not in printed:
            results[name] = []
            printed.add(name)
        results[name].append(ThresholdResult(name=name, status=status, detail=detail))
    return [result for name_results in results.values() for result in name_results]
