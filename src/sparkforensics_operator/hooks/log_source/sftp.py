from __future__ import annotations

import concurrent.futures
from pathlib import Path

from sparkforensics_operator._compat import AirflowException
from sparkforensics_operator.log_ref import LocalEventLog

from ._dest_root import dest_for, make_dest_root, remove_if_owned
from ._path_template import checked_path
from ._rolling_log import _ROLLING_ENTRY_RE
from .base import LogSourceHook


class SFTPLogSourceHook(LogSourceHook):
    """Reads an event log from an on-prem filesystem/HDFS path pattern the
    cloud Airflow worker can't mount directly, over the same SSH connection
    already configured for SSHOperator (via SFTPHook). The SSH-reachable
    equivalent of FilesystemLogSourceHook: unlike that hook, every fetch is
    a remote-to-local copy. Same cleanup() convention as
    HistoryServerLogSourceHook (not FilesystemLogSourceHook): the path is
    auto-cleaned when dest_dir is None (a private temp dir this hook
    owns), but is a no-op when dest_dir is set (a caller-managed shared
    directory)."""

    template_fields = ("ssh_conn_id", "path_template", "dest_dir")

    def __init__(self, ssh_conn_id: str, path_template: str, dest_dir: str | None = None):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.path_template = path_template
        self.dest_dir = dest_dir
        self._owned_temp_root: Path | None = None

    def locate(self, context: dict) -> LocalEventLog:
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        remote_path = checked_path(self.path_template)
        dest_root, self._owned_temp_root = make_dest_root(self.dest_dir)

        try:
            sftp_hook = SFTPHook(ssh_conn_id=self.ssh_conn_id)
            if not sftp_hook.path_exists(remote_path):
                raise AirflowException(
                    f"Configured log path does not exist (checked over SFTP via "
                    f"ssh_conn_id={self.ssh_conn_id!r}): {remote_path}"
                )

            dest_root.mkdir(parents=True, exist_ok=True)

            if sftp_hook.isdir(remote_path):
                entries = sftp_hook.list_directory(remote_path) or []
                rolling_entries = [e for e in entries if _ROLLING_ENTRY_RE.match(Path(e).name)]
                if not rolling_entries:
                    raise AirflowException(
                        f"Unexpected SFTP log directory contents (not a rolling-log "
                        f"layout) at {remote_path!r} (checked over SFTP via "
                        f"ssh_conn_id={self.ssh_conn_id!r}): {entries}"
                    )
                source_name = Path(remote_path).name
                if not source_name:
                    raise AirflowException(
                        f"path_template resolved to a remote directory with no name "
                        f"component to stage locally under dest_dir: {remote_path!r}"
                    )
                # Isolate per-source, same convention as FilesystemLogSourceHook's
                # dest_root / source.name: a caller-supplied dest_dir shared across
                # runs must not mix one run's events_<n>_* segments with another's.
                source_dir = dest_root / source_name
                source_dir.mkdir(parents=True, exist_ok=True)

                def _download(entry: str) -> None:
                    # Re-derive the basename that already passed the
                    # _ROLLING_ENTRY_RE check above and use it (not the raw
                    # entry) for the local destination, so a malformed
                    # directory entry can't escape source_dir.
                    local_name = Path(entry).name
                    remote_file = f"{remote_path.rstrip('/')}/{entry}"
                    sftp_hook.retrieve_file(remote_file, str(source_dir / local_name))

                # Segments are independent files over the same SSH connection;
                # overlapping the transfers cuts wall-clock time for
                # multi-segment rolling logs versus one-at-a-time downloads.
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(8, len(rolling_entries))
                ) as pool:
                    list(pool.map(_download, rolling_entries))
                return LocalEventLog(source_dir)

            source_name = Path(remote_path).name
            if not source_name:
                raise AirflowException(
                    f"path_template resolved to a remote path with no filename "
                    f"component to stage locally under dest_dir: {remote_path!r}"
                )
            local_path = dest_for(dest_root, remote_path)
            sftp_hook.retrieve_file(remote_path, str(local_path))
            return LocalEventLog(local_path)
        except AirflowException:
            remove_if_owned(self._owned_temp_root)
            raise
        except Exception as e:
            remove_if_owned(self._owned_temp_root)
            raise AirflowException(
                f"SFTP log fetch failed (ssh_conn_id={self.ssh_conn_id!r}, "
                f"remote_path={remote_path!r}): {e}"
            ) from e

    def cleanup(self, log_ref: LocalEventLog) -> None:
        remove_if_owned(self._owned_temp_root)
