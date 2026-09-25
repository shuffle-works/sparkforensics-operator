from abc import ABC, abstractmethod
from typing import Sequence

from sparkforensics_operator._compat import BaseHook
from sparkforensics_operator.hooks._templating import TemplatedHookMixin
from sparkforensics_operator.log_ref import EventLogRef


class LogSourceHook(TemplatedHookMixin, BaseHook, ABC):
    """Resolves a Spark job's event log to an EventLogRef the AnalyzeHook
    reads. Implementations differ in where the log comes from: some fetch
    it to the worker (a Spark History Server download, a filesystem/HDFS
    path pattern, an XCom value pushed by the upstream Spark task, SFTP) and
    return a LocalEventLog; others leave it where it is and return a
    reference to it (a path on an SSH host, a History Server application).

    template_fields names the constructor arguments Airflow renders as Jinja
    before locate() runs, when the hook is SparkForensicsOperator's
    log_source (see hooks/_templating.py). The method is locate(), not
    resolve(): Airflow 3's templater calls resolve(context) on any template
    field value that has one, as it does for an XComArg, instead of
    rendering it.
    """

    template_fields: Sequence[str] = ()

    @abstractmethod
    def locate(self, context: dict) -> EventLogRef:
        """Return a reference to the event log: either a single file, or a
        directory of rolling-log segments (files named events_<n>_...), or a
        History Server application."""

    def cleanup(self, log_ref: EventLogRef) -> None:
        """Called after analysis, on success or failure. No-op by default:
        override only to remove a path this hook created for itself (e.g. a
        private temp dir); never one it merely handed back to the caller,
        such as a user-owned filesystem path or another task's
        XCom-referenced file."""
