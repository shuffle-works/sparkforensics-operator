"""Fixtures for the tests that run the real sparkforensics-analyze CLI.

The CLI comes from the npm package `sparkforensics-cli` (on PATH, or at
SPARKFORENSICS_ANALYZE_BIN), and the event logs from the public corpus
repository, fetched once per session at a pinned tag. Without either, these
tests skip, unless SPARKFORENSICS_REQUIRE_CLI=1 (CI sets it), which turns a
missing CLI or an unreachable corpus into a failure, so a broken setup
cannot pass as a skip.
"""
from __future__ import annotations

import io
import os
import shutil
import socket
import threading
import urllib.error
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

CORPUS_TAG = "v2026-09-06"
CORPUS_URL = "https://raw.githubusercontent.com/shuffle-works/spark-event-corpus-data/{tag}/logs/{name}"
REQUIRE_CLI_ENV = "SPARKFORENSICS_REQUIRE_CLI"
ANALYZE_BIN_ENV = "SPARKFORENSICS_ANALYZE_BIN"


def unavailable(reason: str):
    """Skips the test, or fails it when the setup is required."""
    if os.environ.get(REQUIRE_CLI_ENV) == "1":
        pytest.fail(f"{reason} ({REQUIRE_CLI_ENV}=1 makes this a failure)")
    pytest.skip(reason)


@pytest.fixture(scope="session")
def analyze_bin() -> str:
    path = os.environ.get(ANALYZE_BIN_ENV) or shutil.which("sparkforensics-analyze")
    if not path:
        unavailable("sparkforensics-analyze is not installed (npm install -g sparkforensics-cli)")
    return path


@pytest.fixture(scope="session")
def corpus(tmp_path_factory):
    """fetch(name) -> Path of that public corpus log under logs/, downloaded
    once per session."""
    directory = tmp_path_factory.mktemp("corpus")

    def fetch(name: str) -> Path:
        path = directory / Path(name).name
        if not path.exists():
            url = CORPUS_URL.format(tag=CORPUS_TAG, name=name)
            try:
                with urllib.request.urlopen(url, timeout=60) as response:
                    path.write_bytes(response.read())
            except (urllib.error.URLError, TimeoutError) as e:
                unavailable(f"cannot fetch the public corpus log {url}: {e}")
        return path

    return fetch


@pytest.fixture
def clean_log(corpus, analyze_bin) -> Path:
    """A 1.4 KB log of a finished run: one finding and no task data."""
    return corpus("external/external-minimal-eventlog.ndjson")


@pytest.fixture
def failed_log(corpus, analyze_bin) -> Path:
    """A 1.2 KB log of a run whose job failed, a different run from clean_log."""
    return corpus("external/external-failed-eventlog.ndjson")


@pytest.fixture
def task_log(corpus, analyze_bin) -> Path:
    """A 2.2 MB log with stages and tasks, which budgets can measure."""
    return corpus("spark-3.5.9-parquet-baseline.ndjson")


class FakeHistoryServer:
    """A Spark History Server on localhost that serves event logs the way the
    CLI fetches them: GET /api/v1/applications/<id>/logs answers a zip with
    the log in it, and any other application is a 404."""

    def __init__(self):
        self.logs: dict[str, bytes] = {}
        self.requests: list[str] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                owner.requests.append(self.path)
                parts = self.path.split("?")[0].strip("/").split("/")
                app_id = parts[3] if len(parts) > 3 else ""
                if app_id not in owner.logs:
                    self.send_response(404)
                    self.end_headers()
                    return
                archive = io.BytesIO()
                with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
                    zipped.writestr("eventlog", owner.logs[app_id])
                body = archive.getvalue()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def serve(self, app_id: str, log_path: Path) -> None:
        self.logs[app_id] = log_path.read_bytes()

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def history_server():
    with FakeHistoryServer() as server:
        yield server


@pytest.fixture
def closed_port_url() -> str:
    """The address of a port nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}"
