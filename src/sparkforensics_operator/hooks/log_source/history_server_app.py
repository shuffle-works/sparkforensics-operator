from __future__ import annotations

from sparkforensics_operator.log_ref import HistoryServerApp

from ._history_server_args import checked_app_id, optional_attempt_id

from .base import LogSourceHook


class HistoryServerAppLogSourceHook(LogSourceHook):
    """Points at a Spark application on a Spark History Server without
    downloading its event log: sparkforensics-analyze fetches it itself
    (--shs-base-url/--app-id/--attempt-id), from wherever the AnalyzeHook
    runs. So base_url is the History Server's address as seen from that
    host: http://localhost:18080 for SSHAnalyzeHook on the History Server's
    own node, or an address the worker can reach for SubprocessAnalyzeHook.
    Use HistoryServerLogSourceHook instead to download the log to the
    worker first."""

    template_fields = ("base_url", "app_id", "attempt_id")

    def __init__(self, base_url: str, app_id: str, attempt_id: str | None = None):
        super().__init__()
        self.base_url = base_url
        self.app_id = app_id
        self.attempt_id = attempt_id

    def locate(self, context: dict) -> HistoryServerApp:
        return HistoryServerApp(
            base_url=self.base_url.rstrip("/"),
            app_id=checked_app_id(self.app_id),
            attempt_id=optional_attempt_id(self.attempt_id),
        )
