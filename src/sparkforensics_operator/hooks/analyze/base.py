from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from sparkforensics_operator.log_ref import EventLogRef
from sparkforensics_operator.report import Report


class AnalyzeHook(BaseHook, ABC):
    """Runs sparkforensics analysis over an event log and returns a Report.
    Where the analysis runs is the implementation's concern (the Airflow
    worker for SubprocessAnalyzeHook, an SSH host for SSHAnalyzeHook);
    which EventLogRef kinds it can read from there is declared in
    supported_log_refs and enforced by analyze() before any work starts.
    See docs/architecture.md's "Deferred" section for why an HTTP/MCP
    backend is not shipped."""

    supported_log_refs: tuple[type, ...] = ()

    def analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        """thresholds keys: max_runtime_ms, max_spill_gb, max_skew_ratio,
        max_failed_task_rate_pct, min_efficiency_pct (all optional)."""
        if not isinstance(log_ref, self.supported_log_refs):
            supported = ", ".join(t.__name__ for t in self.supported_log_refs) or "none"
            raise AirflowException(
                f"{type(self).__name__} cannot analyze {log_ref!r}; it reads: "
                f"{supported}. Pair it with a log "
                "source that resolves to one of those (see the log source/backend "
                "table in docs/api-reference.md)."
            )
        return self._analyze(log_ref, thresholds)

    @abstractmethod
    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        """Run the analysis; log_ref is already known to be one of
        supported_log_refs."""
