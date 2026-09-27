from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Sequence

from sparkforensics_operator._compat import AirflowException, BaseHook
from sparkforensics_operator.hooks._templating import TemplatedHookMixin
from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp
from sparkforensics_operator.report import Report


class AnalyzeHook(TemplatedHookMixin, BaseHook, ABC):
    """Runs sparkforensics analysis over an event log and returns a Report.
    Where the analysis runs is the implementation's concern (the Airflow
    worker for SubprocessAnalyzeHook, an SSH host for SSHAnalyzeHook);
    which EventLogRef kinds it can read from there is declared in
    supported_log_refs and enforced by analyze() before any work starts.
    See ARCHITECTURE.md's "Not implemented" section for why an HTTP/MCP
    backend is not shipped."""

    supported_log_refs: tuple[type, ...] = ()
    # The EventLogRef kinds this backend can pass the CLI as --baseline,
    # which reads only a file path from where the CLI runs. Empty: this
    # backend cannot compare runs.
    supported_baseline_refs: tuple[type, ...] = ()
    # Constructor arguments Airflow renders as Jinja when the hook is
    # SparkForensicsOperator's backend (see hooks/_templating.py).
    template_fields: Sequence[str] = ()

    def analyze(
        self, log_ref: EventLogRef, thresholds: dict, baseline_ref: EventLogRef | None = None
    ) -> Report:
        """thresholds keys: max_runtime_ms, max_spill_gb, max_skew_ratio,
        max_failed_task_rate_pct, min_efficiency_pct, and, with a
        baseline_ref, max_regression_pct, regression_metric,
        fail_on_introduced (all optional). baseline_ref is the run to
        compare against."""
        self.check_log_ref(log_ref)
        if baseline_ref is None:
            return self._analyze(log_ref, thresholds)
        self.check_baseline_ref(baseline_ref)
        return self._analyze(log_ref, thresholds, baseline_ref=baseline_ref)

    def check_log_ref(self, log_ref: EventLogRef) -> None:
        """Raise unless this backend can read log_ref from where it runs."""
        if not isinstance(log_ref, self.supported_log_refs):
            supported = ", ".join(t.__name__ for t in self.supported_log_refs) or "none"
            raise AirflowException(
                f"{type(self).__name__} cannot analyze {log_ref!r}; it reads: "
                f"{supported}. Pair it with a log "
                "source that resolves to one of those (see the log source/backend "
                "table in docs/configuration.md)."
            )

    def check_baseline_ref(self, baseline_ref: EventLogRef) -> None:
        """Raise unless this backend can pass baseline_ref to the CLI as
        --baseline: a file path readable where the analysis runs."""
        if isinstance(baseline_ref, self.supported_baseline_refs):
            return
        name = type(self).__name__
        if not self.supported_baseline_refs:
            raise AirflowException(f"{name} cannot compare a run against a baseline.")
        if isinstance(baseline_ref, HistoryServerApp):
            problem = (
                "a Spark History Server application cannot be a baseline: "
                "sparkforensics-analyze --baseline reads only an event log file or "
                "rolling-log directory, never a History Server"
            )
        else:
            problem = f"it reads a baseline only as {self._baseline_ref_names()}"
        raise AirflowException(
            f"{name} cannot use {baseline_ref!r} as the baseline; {problem}. Point "
            "baseline_log_source at a log source that resolves to "
            f"{self._baseline_ref_names()}."
        )

    def _baseline_ref_names(self) -> str:
        return " or ".join(t.__name__ for t in self.supported_baseline_refs)

    @abstractmethod
    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        """Run the analysis; log_ref is already known to be one of
        supported_log_refs. A backend with supported_baseline_refs also
        takes a baseline_ref keyword, already known to be one of those, and
        receives it only when there is a baseline."""

    def on_kill(self) -> None:
        """Called from SparkForensicsOperator.on_kill() when the task is
        killed while analyze() runs in this process, to stop the analysis.
        No-op by default."""


class DeferrableAnalyzeHook(AnalyzeHook):
    """An AnalyzeHook that can also run the analysis as a detached job, so
    SparkForensicsOperator(deferrable=True) frees its worker slot while the
    job runs: submit() starts it, the operator defers on trigger_for(),
    and a fresh operator instance calls collect() on a worker once the
    trigger fires. Nothing but the job dict submit() returns crosses the
    deferral, so it must hold only JSON-native values and everything
    collect() and abandon() need. See ARCHITECTURE.md's "Deferrable
    execution" section."""

    def cannot_defer_reason(self) -> str | None:
        """Why this backend cannot run detached in this environment (e.g. a
        dependency too old), or None when it can."""
        return None

    @abstractmethod
    def submit(self, log_ref: EventLogRef, thresholds: dict, context: Any) -> dict:
        """Start the analysis detached and return the job. log_ref is
        already known to be one of supported_log_refs. As with _analyze(),
        a baseline_ref keyword, already checked, is passed only when there
        is a baseline. Must first stop and
        remove whatever an earlier try of the same task instance left
        behind, so a retry or clear never runs two analyses at once."""

    @abstractmethod
    def trigger_for(self, job: dict) -> Any:
        """The trigger to defer on until the job finishes."""

    @abstractmethod
    def defer_timeout(self, job: dict) -> timedelta:
        """How much longer to wait for the job before giving up on it: a
        backstop for a job that never reports back, past the job's own
        time limit."""

    @abstractmethod
    def collect(self, job: dict, event: dict, log_ref: EventLogRef, thresholds: dict) -> Report:
        """Read the finished job's result back into a Report, raising the
        same errors analyze() raises for the same outcome, and remove the
        job's remote state on success and on failure."""

    @abstractmethod
    def abandon(self, context: Any, job: dict | None = None) -> None:
        """Stop and remove any job of this task instance, when the
        deferral failed or timed out, or the task was killed, and collect()
        will not run. job is the one submit() returned, when the caller
        still has it; without it, the job is found from context and this
        hook's own configuration."""
