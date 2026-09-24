from __future__ import annotations

from typing import Sequence

from sparkforensics_operator import sinks
from sparkforensics_operator._compat import BaseOperator
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.links import ReportLink

_THRESHOLD_BREACH_ACTIONS = ("fail", "warn", "ignore")


def _validate_on_threshold_breach(value: str) -> None:
    if value not in _THRESHOLD_BREACH_ACTIONS:
        raise ValueError(
            f"on_threshold_breach must be one of {_THRESHOLD_BREACH_ACTIONS}, got {value!r}"
        )


def run_spark_forensics(
    context: dict,
    *,
    log_source,
    backend,
    report_dest: str,
    thresholds: dict,
    on_threshold_breach: str,
    notifier,
    log,
    aws_conn_id: str | None = None,
) -> str:
    """The one code path shared by SparkForensicsOperator.execute() and the
    spark_forensics_callback() factory (Task 15)."""
    _validate_on_threshold_breach(on_threshold_breach)
    log_path = log_source.fetch(context)
    try:
        report = backend.analyze(log_path, thresholds)

        report_json = {
            "schemaVersion": report.schema_version,
            "summary": report.summary,
            "evidenceAvailability": report.evidence_availability,
            "detectors": report.detectors,
            "findings": report.findings,
            "recommendations": report.recommendations,
            "cleanChecks": report.clean_checks,
            "thresholdResults": [
                {"name": r.name, "status": r.status, "detail": r.detail}
                for r in report.threshold_results
            ],
        }
        destination = sinks.persist(report_json, report_dest, aws_conn_id=aws_conn_id)

        for result in report.threshold_results:
            if result.status == "inconclusive":
                log.warning("SparkForensics threshold %s inconclusive: %s", result.name, result.detail)

        if not report.inconclusive and report.exit_code == 3:
            # The CLI's exit code contract says 3 means thresholds were
            # inconclusive (see the plan's Global Constraints), but nothing in
            # stderr matched the threshold-line regex -- e.g. its stderr
            # format drifted. Log a warning rather than silently saying
            # nothing, matching the exit-code-1 fallback below. This is
            # log-only: exit code 3 is not a violation, so it must not affect
            # `violated`/the fail/warn/ignore branching below.
            log.warning(
                "SparkForensics thresholds were inconclusive per exit code 3, but the "
                "detail could not be parsed from stderr"
            )

        if notifier is not None:
            notifier.notify(report, destination)

        violated = report.violated
        message = ""
        if violated:
            breached = [r for r in report.threshold_results if r.status == "violation"]
            message = "; ".join(f"{r.name}: {r.detail}" for r in breached)
        elif report.exit_code == 1:
            # The CLI's exit code contract says 1 means a threshold was violated
            # (see the plan's Global Constraints), but nothing in stderr matched
            # the threshold-line regex -- e.g. its stderr format drifted. Treat
            # this the same as a parsed violation rather than silently passing.
            violated = True
            message = (
                "thresholds were violated per exit code 1, but the violation "
                "could not be parsed from stderr"
            )

        if violated:
            if on_threshold_breach == "fail":
                # The report was already persisted above (destination is
                # known-good at this point); attach it to the raised
                # exception so callers that can't get a return value from
                # this call on the breach path (spark_forensics_callback's
                # _callback, see callback.py) can still recover it, e.g. to
                # push it to XCom.
                breach = ThresholdBreached(f"SparkForensics threshold(s) breached: {message}")
                breach.destination = destination
                raise breach
            if on_threshold_breach == "warn":
                log.warning("SparkForensics threshold(s) breached: %s", message)
            # "ignore": no-op.

        return destination
    finally:
        log_source.cleanup(log_path)


class SparkForensicsOperator(BaseOperator):
    """Fetches a Spark job's event log, analyzes it with sparkforensics,
    persists the report, evaluates thresholds, and optionally notifies via
    a caller-supplied Notifier. log_source and backend are pluggable
    strategy Hooks (see hooks/log_source/* and hooks/analyze/*)."""

    operator_extra_links = (ReportLink(),)
    template_fields: Sequence[str] = ("report_dest",)

    def __init__(
        self,
        *,
        log_source,
        backend,
        report_dest: str,
        max_runtime_ms: int | None = None,
        max_spill_gb: float | None = None,
        max_skew_ratio: float | None = None,
        max_failed_task_rate_pct: float | None = None,
        min_efficiency_pct: float | None = None,
        on_threshold_breach: str = "fail",
        notifier=None,
        aws_conn_id: str | None = None,
        **kwargs,
    ):
        # Validate before super().__init__() to avoid DAG registration side effects if construction will fail.
        _validate_on_threshold_breach(on_threshold_breach)
        super().__init__(**kwargs)
        self.log_source = log_source
        self.backend = backend
        self.report_dest = report_dest
        self.thresholds = {
            "max_runtime_ms": max_runtime_ms,
            "max_spill_gb": max_spill_gb,
            "max_skew_ratio": max_skew_ratio,
            "max_failed_task_rate_pct": max_failed_task_rate_pct,
            "min_efficiency_pct": min_efficiency_pct,
        }
        self.on_threshold_breach = on_threshold_breach
        self.notifier = notifier
        self.aws_conn_id = aws_conn_id
        # Where this run's report was persisted; read by ReportLink on Airflow 3.
        self.persisted_report_dest: str | None = None

    def execute(self, context: dict) -> str:
        try:
            destination = run_spark_forensics(
                context,
                log_source=self.log_source,
                backend=self.backend,
                report_dest=self.report_dest,
                thresholds=self.thresholds,
                on_threshold_breach=self.on_threshold_breach,
                notifier=self.notifier,
                log=self.log,
                aws_conn_id=self.aws_conn_id,
            )
        except ThresholdBreached as breach:
            self.persisted_report_dest = breach.destination
            raise
        self.persisted_report_dest = destination
        return destination
