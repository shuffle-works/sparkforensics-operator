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


def log_ref_to_dict(log_ref: EventLogRef) -> dict:
    """A JSON-native form of log_ref (str/None values only), for state that
    must cross a deferral: Airflow serializes a deferred task's resume
    kwargs, and dataclasses don't survive that on every version. Only the
    references a deferrable backend reads (RemoteEventLog, HistoryServerApp)
    are supported."""
    if isinstance(log_ref, RemoteEventLog):
        return {"kind": "remote", "ssh_conn_id": log_ref.ssh_conn_id, "path": log_ref.path}
    if isinstance(log_ref, HistoryServerApp):
        return {
            "kind": "history_server_app",
            "base_url": log_ref.base_url,
            "app_id": log_ref.app_id,
            "attempt_id": log_ref.attempt_id,
        }
    raise TypeError(f"Not an event log reference: {log_ref!r}")


def log_ref_from_dict(data: dict) -> EventLogRef:
    """Inverse of log_ref_to_dict."""
    kind = data.get("kind")
    if kind == "remote":
        return RemoteEventLog(ssh_conn_id=data["ssh_conn_id"], path=data["path"])
    if kind == "history_server_app":
        return HistoryServerApp(
            base_url=data["base_url"], app_id=data["app_id"], attempt_id=data.get("attempt_id")
        )
    raise ValueError(f"Not a serialized event log reference: {data!r}")
