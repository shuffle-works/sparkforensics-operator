from abc import ABC, abstractmethod
from pathlib import Path

from airflow.hooks.base import BaseHook

from sparkforensics_operator.report import Report


class AnalyzeHook(BaseHook, ABC):
    """Runs sparkforensics analysis over a local event log and returns a
    Report. v1 ships one implementation (subprocess against the
    sparkforensics-analyze CLI), see the plan's "Future: http/MCP
    AnalyzeHook backend" section for why an HTTP backend was deferred."""

    @abstractmethod
    def analyze(self, log_path: Path, thresholds: dict) -> Report:
        """thresholds keys: max_runtime_ms, max_spill_gb, max_skew_ratio,
        max_failed_task_rate_pct, min_efficiency_pct (all optional)."""
