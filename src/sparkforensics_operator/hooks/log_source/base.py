from abc import ABC, abstractmethod
from pathlib import Path

from airflow.hooks.base import BaseHook


class LogSourceHook(BaseHook, ABC):
    """Fetches a Spark job's event log to a local path. Implementations
    differ only in where the log comes from: a Spark History Server, a
    filesystem/HDFS path pattern, or an XCom value pushed by the upstream
    Spark task.
    """

    @abstractmethod
    def fetch(self, context: dict) -> Path:
        """Return a local path to the event log: either a single file, or a
        directory of rolling-log segments (files named events_<n>_...)."""

    def cleanup(self, path: Path) -> None:
        """Called after analysis, on success or failure. No-op by default:
        override only to remove a path this hook created for itself (e.g. a
        private temp dir); never one it merely handed back to the caller,
        such as a user-owned filesystem path or another task's
        XCom-referenced file."""
