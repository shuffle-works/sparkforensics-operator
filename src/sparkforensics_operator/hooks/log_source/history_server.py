from __future__ import annotations

import time
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import quote

import requests
from sparkforensics_operator._compat import AirflowException
from sparkforensics_operator.log_ref import LocalEventLog

from ._dest_root import make_dest_root, remove_if_owned
from ._history_server_args import checked_app_id, optional_attempt_id
from ._rolling_log import _ROLLING_ENTRY_RE
from .base import LogSourceHook


class HistoryServerLogSourceHook(LogSourceHook):
    """Downloads a Spark job's event log from a Spark History Server's REST
    API: GET {base_url}/api/v1/applications/{app_id}[/{attempt_id}]/logs,
    which always returns a zip (one bare entry for a single event-log file;
    for a rolling log, the events_<n>_... segments under one
    eventlog_v2_<appId>/ folder)."""

    template_fields = ("base_url", "app_id", "attempt_id", "dest_dir")

    def __init__(
        self,
        base_url: str,
        app_id: str,
        attempt_id: str | None = None,
        dest_dir: str | None = None,
        timeout: int = 300,
    ):
        super().__init__()
        self.base_url = base_url
        self.app_id = app_id
        self.attempt_id = attempt_id
        self.dest_dir = dest_dir
        self.timeout = timeout
        self._owned_temp_root: Path | None = None

    def _build_url(self) -> str:
        segments = ["api", "v1", "applications", quote(checked_app_id(self.app_id), safe="")]
        attempt_id = optional_attempt_id(self.attempt_id)
        if attempt_id:
            segments.append(quote(attempt_id, safe=""))
        segments.append("logs")
        return f"{self.base_url.rstrip('/')}/{'/'.join(segments)}"

    def locate(self, context: dict) -> LocalEventLog:
        url = self._build_url()
        response = requests.get(url, timeout=self.timeout, stream=True)
        if response.status_code != 200:
            raise AirflowException(
                f"Spark History Server log download failed ({response.status_code}) for {url}"
            )

        dest_root, self._owned_temp_root = make_dest_root(self.dest_dir)

        try:
            dest_root.mkdir(parents=True, exist_ok=True)

            # self.app_id is only safe as a URL segment (quoted above); sanitize
            # it before reusing it as a filesystem path component so a
            # crafted/unexpected app_id (e.g. containing "../") can't escape
            # dest_root.
            safe_app_id = Path(self.app_id).name
            zip_path = dest_root / f"{safe_app_id}.zip"
            # self.timeout only bounds each individual socket read/connect (see
            # the requests.get call above); a slow-trickling connection could
            # otherwise keep the download running far longer than self.timeout
            # implies. Track an explicit wall-clock deadline so self.timeout
            # bounds the entire transfer, not just each chunk read.
            deadline = time.monotonic() + self.timeout
            with open(zip_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)
                    if time.monotonic() > deadline:
                        response.close()
                        raise AirflowException(
                            f"Spark History Server log download for app {self.app_id} "
                            f"exceeded {self.timeout}s (total transfer time, not just a "
                            "single read/connect)."
                        )

            extract_dir = dest_root / safe_app_id
            with zipfile.ZipFile(zip_path) as zf:
                names = zf.namelist()
                zf.extractall(extract_dir)
            zip_path.unlink()

            # Zip directory entries ("folder/") carry no data; judge the layout
            # by the file entries alone.
            file_names = [name for name in names if not name.endswith("/")]
            if len(file_names) == 1 and "/" not in file_names[0]:
                return LocalEventLog(extract_dir / file_names[0])
            return LocalEventLog(extract_dir / self._rolling_log_folder(file_names))
        except Exception:
            remove_if_owned(self._owned_temp_root)
            raise

    def _rolling_log_folder(self, file_names: list[str]) -> str:
        """Returns the one folder a rolling-log zip's entries live under.

        Spark's RollingEventLogFilesFileReader.zipEventLogFiles writes every
        entry as eventlog_v2_<appId>/<file>. sparkforensics-analyze only
        checks the direct children of the path it's given for events_<n>_
        files, so the caller must hand it that folder, not the extraction
        root. Any other layout raises rather than guessing which folder holds
        the log."""
        parts = [PurePosixPath(name).parts for name in file_names]
        folders = {entry_parts[0] for entry_parts in parts if len(entry_parts) == 2}
        if len(folders) != 1 or any(len(entry_parts) != 2 for entry_parts in parts):
            raise AirflowException(
                f"Unexpected Spark History Server log archive contents for app {self.app_id}: "
                "expected a single event-log file or a rolling log whose entries are all "
                "directly under one eventlog_v2_<appId>/ folder, but the entries are not "
                f"under exactly one folder: {file_names}"
            )
        (folder,) = folders
        if folder in ("/", ".", "..") or not any(
            _ROLLING_ENTRY_RE.match(entry_parts[1]) for entry_parts in parts
        ):
            raise AirflowException(
                f"Unexpected Spark History Server log archive contents for app {self.app_id}: "
                f"folder {folder!r} has no events_<n>_ rolling-log entries: {file_names}"
            )
        return folder

    def cleanup(self, log_ref: LocalEventLog) -> None:
        remove_if_owned(self._owned_temp_root)
