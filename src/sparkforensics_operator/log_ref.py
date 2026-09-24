"""
Event log references: what a LogSourceHook resolves and an AnalyzeHook
consumes. A reference says where the log is, which is independent of where
the analysis runs: a log can stay on an SSH host or behind a Spark History
Server and never reach the Airflow worker, as long as the backend that
analyzes it can read it from there.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Union


@dataclass(frozen=True)
class LocalEventLog:
    """An event log file, or rolling-log directory, on the Airflow worker's
    own filesystem."""

    path: Path

    def describe(self) -> str:
        return str(self.path)


@dataclass(frozen=True)
class RemoteEventLog:
    """An event log file, or rolling-log directory, on the host behind the
    ssh_conn_id Airflow connection. Only a backend that runs on that same
    host can read it."""

    ssh_conn_id: str
    path: str

    def describe(self) -> str:
        return f"{self.path} on ssh_conn_id={self.ssh_conn_id!r}"


@dataclass(frozen=True)
class HistoryServerApp:
    """A Spark application on a Spark History Server, which
    sparkforensics-analyze fetches itself (--shs-base-url/--app-id/
    --attempt-id). base_url must be reachable from wherever the analysis
    runs, e.g. http://localhost:18080 when the backend runs on the History
    Server's own host."""

    base_url: str
    app_id: str
    attempt_id: str | None = None

    def describe(self) -> str:
        attempt = f" attempt {self.attempt_id}" if self.attempt_id else ""
        return f"app {self.app_id}{attempt} on {self.base_url}"


EventLogRef = Union[LocalEventLog, RemoteEventLog, HistoryServerApp]
