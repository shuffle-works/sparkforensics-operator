from __future__ import annotations

import logging
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)


class Notifier(ABC):
    """Base class for delivering a SparkForensics report summary to any
    messaging or paging system (Slack, MS Teams, email, PagerDuty, ZenDuty,
    ...). Implement _send(); this package ships no concrete notifiers, only
    this interface, since maintaining channel integrations is out of scope
    for a Spark-log-analysis package.

    notify() wraps _send() so a delivery failure is logged as a warning and
    swallowed, never raised, a notification failure must never fail the
    SparkForensics task."""

    def notify(self, report, report_destination: str) -> None:
        try:
            self._send(report, report_destination)
        except Exception:
            log.warning("SparkForensics notification failed; continuing.", exc_info=True)

    @abstractmethod
    def _send(self, report, report_destination: str) -> None:
        """Deliver the report to whatever channel this implements."""

    @staticmethod
    def _default_message(report, report_destination: str) -> str:
        counts = report.summary.get("impactBandCounts", {})
        return (
            f"SparkForensics report: {counts.get('critical', 0)} critical, "
            f"{counts.get('warning', 0)} warning, {counts.get('info', 0)} info findings. "
            f"Report: {report_destination}"
        )
