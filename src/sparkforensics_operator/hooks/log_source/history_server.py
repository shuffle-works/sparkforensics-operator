from __future__ import annotations

import time
import zipfile
from pathlib import Path
from urllib.parse import quote

import requests
from airflow.exceptions import AirflowException

from ._dest_root import make_dest_root, remove_if_owned
from ._rolling_log import _ROLLING_ENTRY_RE
from .base import LogSourceHook


class HistoryServerLogSourceHook(LogSourceHook):
    """Downloads a Spark job's event log from a Spark History Server's REST
    API: GET {base_url}/api/v1/applications/{app_id}[/{attempt_id}]/logs,
    which always returns a zip (one entry for a single event-log file,
    multiple events_<n>_... entries for a rolling log)."""

    def __init__(
        self,
        base_url: str,
        app_id: str,
        attempt_id: str | None = None,
        dest_dir: str | None = None,
        timeout: int = 300,
    ):
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self.app_id = app_id
        self.attempt_id = attempt_id
        self.dest_dir = dest_dir
        self.timeout = timeout
        self._owned_temp_root: Path | None = None

    def _build_url(self) -> str:
        segments = ["api", "v1", "applications", quote(self.app_id, safe="")]
        if self.attempt_id:
            segments.append(quote(self.attempt_id, safe=""))
        segments.append("logs")
        return f"{self.base_url}/{'/'.join(segments)}"

    def fetch(self, context: dict) -> Path:
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

            if len(names) == 1:
                return extract_dir / names[0]
            if any(_ROLLING_ENTRY_RE.match(Path(name).name) for name in names):
                return extract_dir
            raise AirflowException(
                f"Unexpected Spark History Server log archive contents for app {self.app_id}: {names}"
            )
        except Exception:
            remove_if_owned(self._owned_temp_root)
            raise

    def cleanup(self, path: Path) -> None:
        remove_if_owned(self._owned_temp_root)
