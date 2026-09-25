from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Sequence

from sparkforensics_operator._compat import AirflowException, BaseHook
from sparkforensics_operator.hooks._templating import TemplatedHookMixin
from sparkforensics_operator.log_ref import EventLogRef
from sparkforensics_operator.report import Report


class AnalyzeHook(TemplatedHookMixin, BaseHook, ABC):
    """Runs sparkforensics analysis over an event log and returns a Report.
    Where the analysis runs is the implementation's concern (the Airflow
    worker for SubprocessAnalyzeHook, an SSH host for SSHAnalyzeHook);
    which EventLogRef kinds it can read from there is declared in
    supported_log_refs and enforced by analyze() before any work starts.
    See docs/architecture.md's "Deferred" section for why an HTTP/MCP
    backend is not shipped."""

    supported_log_refs: tuple[type, ...] = ()
    # Constructor arguments Airflow renders as Jinja when the hook is
    # SparkForensicsOperator's backend (see hooks/_templating.py).
    template_fields: Sequence[str] = ()

    def analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        """thresholds keys: max_runtime_ms, max_spill_gb, max_skew_ratio,
        max_failed_task_rate_pct, min_efficiency_pct (all optional)."""
        self.check_log_ref(log_ref)
        return self._analyze(log_ref, thresholds)

    def check_log_ref(self, log_ref: EventLogRef) -> None:
        """Raise unless this backend can read log_ref from where it runs."""
        if not isinstance(log_ref, self.supported_log_refs):
            supported = ", ".join(t.__name__ for t in self.supported_log_refs) or "none"
            raise AirflowException(
                f"{type(self).__name__} cannot analyze {log_ref!r}; it reads: "
                f"{supported}. Pair it with a log "
                "source that resolves to one of those (see the log source/backend "
                "table in docs/api-reference.md)."
            )

    @abstractmethod
    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        """Run the analysis; log_ref is already known to be one of
        supported_log_refs."""

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
    collect() and abandon() need. See docs/architecture.md's "Deferrable
    execution" section."""

    def cannot_defer_reason(self) -> str | None:
        """Why this backend cannot run detached in this environment (e.g. a
        dependency too old), or None when it can."""
        return None

    @abstractmethod
    def submit(self, log_ref: EventLogRef, thresholds: dict, context: Any) -> dict:
        """Start the analysis detached and return the job. log_ref is
        already known to be one of supported_log_refs. Must first stop and
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
