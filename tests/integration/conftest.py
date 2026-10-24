"""Fixtures for the tests that run the real sparkforensics-analyze CLI.

The CLI comes from the npm package `sparkforensics-cli` (on PATH, or at
SPARKFORENSICS_ANALYZE_BIN), and the event logs from the public corpus
repository, fetched once per session at a pinned tag. Without either, these
tests skip, unless SPARKFORENSICS_REQUIRE_CLI=1 (CI sets it), which turns a
missing CLI or an unreachable corpus into a failure, so a broken setup
cannot pass as a skip.
"""
from __future__ import annotations

import os
import shutil
import urllib.error
import urllib.request
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
def task_log(corpus, analyze_bin) -> Path:
    """A 2.2 MB log with stages and tasks, which budgets can measure."""
    return corpus("spark-3.5.9-parquet-baseline.ndjson")
