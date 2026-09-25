from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException

from sparkforensics_operator import sinks
from sparkforensics_operator._compat import BaseOperator
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze.base import DeferrableAnalyzeHook
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.log_ref import log_ref_from_dict, log_ref_to_dict

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
    # Where the log is (log_source) and where it is analyzed (backend) are
    # independent: the reference may point at a file the worker fetched, a
    # path on an SSH host, or a History Server app the backend reads itself.
    log_ref = log_source.resolve(context)
    try:
        report = backend.analyze(log_ref, thresholds)
        return handle_report(
            report,
            report_dest=report_dest,
            on_threshold_breach=on_threshold_breach,
            notifier=notifier,
            log=log,
            aws_conn_id=aws_conn_id,
        )
    finally:
        log_source.cleanup(log_ref)


def handle_report(
    report,
    *,
    report_dest: str,
    on_threshold_breach: str,
    notifier,
    log,
    aws_conn_id: str | None = None,
) -> str:
    """Persists the report, logs inconclusive thresholds, notifies, and
    applies on_threshold_breach. Shared by run_spark_forensics and the
    deferrable operator's resume step, so a deferred run acts on its report
    exactly as a synchronous one does."""
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


class SparkForensicsOperator(BaseOperator):
    """Resolves a Spark job's event log, analyzes it with sparkforensics,
    persists the report, evaluates thresholds, and optionally notifies via
    a caller-supplied Notifier. log_source (where the log is) and backend
    (where the analysis runs) are pluggable strategy Hooks (see
    hooks/log_source/* and hooks/analyze/*), composed through the
    EventLogRef the log source resolves (see log_ref.py).

    deferrable=True runs the analysis as a detached job and defers until it
    finishes, freeing the worker slot meanwhile; the backend must be a
    DeferrableAnalyzeHook (SSHAnalyzeHook). Left unset, it follows
    Airflow's [operators] default_deferrable, applied only when the backend
    can defer."""

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
        deferrable: bool | None = None,
        **kwargs,
    ):
        # Validate before super().__init__() to avoid DAG registration side effects if construction will fail.
        _validate_on_threshold_breach(on_threshold_breach)
        can_defer = isinstance(backend, DeferrableAnalyzeHook)
        if deferrable and not can_defer:
            raise ValueError(
                f"deferrable=True needs a backend that can run the analysis detached "
                f"(SSHAnalyzeHook); {type(backend).__name__} runs it in the task's own "
                "process, so there is nothing to defer on. Use SSHAnalyzeHook, or drop "
                "deferrable=True."
            )
        if deferrable is None:
            deferrable = can_defer and conf.getboolean(
                "operators", "default_deferrable", fallback=False
            )
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
        self.deferrable = deferrable
        # Where this run's report was persisted; read by ReportLink on Airflow 3.
        self.persisted_report_dest: str | None = None

    def execute(self, context: dict) -> str:
        if self.deferrable:
            self._submit_and_defer(context)  # raises TaskDeferred
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

    def _submit_and_defer(self, context: dict) -> None:
        log_ref = self.log_source.resolve(context)
        try:
            self.backend.check_log_ref(log_ref)
            job = self.backend.submit(log_ref, self.thresholds, context)
        except BaseException:
            self.log_source.cleanup(log_ref)
            raise
        # Everything execute_complete() needs travels in these kwargs, which
        # Airflow serializes with the deferral; the operator that resumes is
        # a fresh instance rebuilt from the DAG file. They carry the values
        # this try rendered and submitted with, so a DAG edited while the
        # task is deferred can't pair this job's output with other settings.
        # Only notifier and on_threshold_breach come from the fresh instance.
        self.defer(
            trigger=self.backend.trigger_for(job),
            method_name="execute_complete",
            kwargs={
                "job": job,
                "log_ref": log_ref_to_dict(log_ref),
                "report_dest": self.report_dest,
                "thresholds": dict(self.thresholds),
            },
            timeout=self._defer_timeout(job, context),
        )

    def _defer_timeout(self, job: dict, context: dict) -> timedelta:
        # Airflow 2 caps a deferral at execution_timeout itself; Airflow 3's
        # task runner takes the deferral's timeout as-is, so cap it here.
        timeout = self.backend.defer_timeout(job)
        if self.execution_timeout is None:
            return timeout
        now = datetime.now(timezone.utc)
        started = getattr(context.get("ti"), "start_date", None) or now
        remaining = started + self.execution_timeout - now
        return max(min(timeout, remaining), timedelta(seconds=1))

    def execute_complete(
        self,
        context: dict,
        event: dict | None = None,
        *,
        job: dict,
        log_ref: dict,
        report_dest: str,
        thresholds: dict,
    ) -> str:
        """Resumes a deferred run on a worker once the trigger fires: reads
        the report back and acts on it exactly as execute() does."""
        if not event:
            raise AirflowException("SparkForensics deferred analysis resumed without a trigger event")
        for line in (event.get("log_chunk") or "").splitlines():
            self.log.info("[remote] %s", line)

        resolved_ref = log_ref_from_dict(log_ref)
        try:
            report = self.backend.collect(job, event, resolved_ref, thresholds)
            destination = handle_report(
                report,
                report_dest=report_dest,
                on_threshold_breach=self.on_threshold_breach,
                notifier=self.notifier,
                log=self.log,
                aws_conn_id=self.aws_conn_id,
            )
        except ThresholdBreached as breach:
            self.persisted_report_dest = breach.destination
            raise
        finally:
            self.log_source.cleanup(resolved_ref)
        self.persisted_report_dest = destination
        return destination

    def resume_execution(self, next_method: str, next_kwargs: dict | None, context: dict):
        # Airflow resumes a deferral that timed out (the backend's
        # defer_timeout or this task's execution_timeout) or whose trigger
        # crashed with next_method="__fail__", and never calls
        # execute_complete(): stop and remove the remote job here before
        # Airflow raises, so nothing keeps running or lingers on the host.
        if next_method == "__fail__" and isinstance(self.backend, DeferrableAnalyzeHook):
            error = (next_kwargs or {}).get("error", "unknown error")
            self.log.error(
                "SparkForensics deferred analysis did not finish (%s); stopping and "
                "removing the remote job.", error,
            )
            try:
                self.backend.abandon(context)
            except Exception:
                self.log.warning(
                    "Could not stop and remove the remote job; the next try of this "
                    "task does.", exc_info=True,
                )
        return super().resume_execution(next_method, next_kwargs, context)
