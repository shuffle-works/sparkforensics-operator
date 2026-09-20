from __future__ import annotations

import shutil
from pathlib import Path

from airflow.exceptions import AirflowException

from ._dest_root import dest_for
from ._path_template import _template_vars
from .base import LogSourceHook


class FilesystemLogSourceHook(LogSourceHook):
    """Reads an event log from a configured filesystem/HDFS path pattern.
    "HDFS" here means an already-mounted path (NFS gateway, WebHDFS FUSE
    mount, or similar), this hook does no Hadoop-client I/O of its own, it
    just reads/copies a local path, the same way FilesystemLogSourceHook's
    output is handed straight to sparkforensics-analyze's own local-file
    reader."""

    def __init__(self, path_template: str, dest_dir: str | None = None):
        super().__init__()
        self.path_template = path_template
        self.dest_dir = dest_dir

    def fetch(self, context: dict) -> Path:
        rendered = self.path_template.format(**_template_vars(context))
        source = Path(rendered)
        if not source.exists():
            raise AirflowException(f"Configured log path does not exist: {source}")
        if self.dest_dir is None:
            return source

        dest_root = Path(self.dest_dir)
        dest_root.mkdir(parents=True, exist_ok=True)
        dest = dest_for(dest_root, source)
        if source.is_dir():
            shutil.copytree(source, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(source, dest)
        return dest

    def cleanup(self, path: Path) -> None:
        if self.dest_dir is None:
            return  # fetch() returned the caller's own path; never delete it.
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)
