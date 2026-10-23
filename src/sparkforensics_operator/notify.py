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
        """One plain-text line: the app, the CLI's verdict, the run time, the
        finding counts, the number of event log lines the CLI skipped when
        there are any, and where the report is. Whatever the report does not
        carry is left out."""
        counts = report.summary.get("impactBandCounts", {})
        parts = [f"SparkForensics report{f' for {report.app_name}' if report.app_name else ''}:"]
        if report.verdict_title:
            parts.append(f"{report.verdict_title.rstrip('.')}.")
        if report.run_time_ms is not None:
            parts.append(f"Ran for {_format_duration(report.run_time_ms)}.")
        parts.append(
            f"{counts.get('critical', 0)} critical, {counts.get('warning', 0)} warning, "
            f"{counts.get('info', 0)} info findings."
        )
        if report.skipped_lines:
            parts.append(f"{report.skipped_lines} event log lines were skipped.")
        parts.append(f"Report: {report_destination}")
        return " ".join(parts)


def _format_duration(milliseconds: float) -> str:
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f} s"
    minutes, seconds = divmod(round(seconds), 60)
    if minutes < 60:
        return f"{minutes} min {seconds} s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} h {minutes} min"
