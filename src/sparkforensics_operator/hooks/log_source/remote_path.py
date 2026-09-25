from __future__ import annotations

from sparkforensics_operator.log_ref import RemoteEventLog

from ._path_template import checked_path
from .base import LogSourceHook


class RemotePathLogSourceHook(LogSourceHook):
    """Points at an event log that stays on the host behind ssh_conn_id,
    for an AnalyzeHook that runs there too (SSHAnalyzeHook with the same
    ssh_conn_id). Nothing is copied to the worker and nothing is checked
    over SSH here: sparkforensics-analyze reports a missing or unreadable
    path itself. path_template is a Jinja template, like
    FilesystemLogSourceHook's/SFTPLogSourceHook's. Use SFTPLogSourceHook
    instead to copy the log to the worker and analyze it there."""

    template_fields = ("ssh_conn_id", "path_template")

    def __init__(self, ssh_conn_id: str, path_template: str):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.path_template = path_template

    def locate(self, context: dict) -> RemoteEventLog:
        return RemoteEventLog(ssh_conn_id=self.ssh_conn_id, path=checked_path(self.path_template))
