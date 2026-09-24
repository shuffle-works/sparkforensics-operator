from abc import ABC, abstractmethod

from airflow.hooks.base import BaseHook

from sparkforensics_operator.log_ref import EventLogRef


class LogSourceHook(BaseHook, ABC):
    """Resolves a Spark job's event log to an EventLogRef the AnalyzeHook
    reads. Implementations differ in where the log comes from: some fetch
    it to the worker (a Spark History Server download, a filesystem/HDFS
    path pattern, an XCom value pushed by the upstream Spark task, SFTP) and
    return a LocalEventLog; others leave it where it is and return a
    reference to it (a path on an SSH host, a History Server application).
    """

    @abstractmethod
    def resolve(self, context: dict) -> EventLogRef:
        """Return a reference to the event log: either a single file, or a
        directory of rolling-log segments (files named events_<n>_...), or a
        History Server application."""

    def cleanup(self, log_ref: EventLogRef) -> None:
        """Called after analysis, on success or failure. No-op by default:
        override only to remove a path this hook created for itself (e.g. a
        private temp dir); never one it merely handed back to the caller,
        such as a user-owned filesystem path or another task's
        XCom-referenced file."""
