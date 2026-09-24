from __future__ import annotations

from airflow.exceptions import AirflowException

from sparkforensics_operator.log_ref import RemoteEventLog

from ._path_template import _template_vars
from .base import LogSourceHook


class RemotePathLogSourceHook(LogSourceHook):
    """Points at an event log that stays on the host behind ssh_conn_id,
    for an AnalyzeHook that runs there too (SSHAnalyzeHook with the same
    ssh_conn_id). Nothing is copied to the worker and nothing is checked
    over SSH here: sparkforensics-analyze reports a missing or unreadable
    path itself. path_template supports the same substitutions as
    FilesystemLogSourceHook/SFTPLogSourceHook. Use SFTPLogSourceHook instead
    to copy the log to the worker and analyze it there."""

    def __init__(self, ssh_conn_id: str, path_template: str):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.path_template = path_template

    def resolve(self, context: dict) -> RemoteEventLog:
        remote_path = self.path_template.format(**_template_vars(context))
        if not remote_path.strip():
            raise AirflowException(
                f"path_template {self.path_template!r} resolved to an empty remote path."
            )
        return RemoteEventLog(ssh_conn_id=self.ssh_conn_id, path=remote_path)
