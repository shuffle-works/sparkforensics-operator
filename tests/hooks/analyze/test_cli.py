from pathlib import Path

import pytest

from sparkforensics_operator.hooks.analyze._cli import build_cli_args
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog


def test_build_cli_args_writes_to_stdout_without_an_out_path():
    args = build_cli_args("sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_spill_gb": 2})

    assert args == ["sparkforensics-analyze", "/tmp/app.log", "--format", "json", "--max-spill", "2"]


def test_build_cli_args_rejects_something_that_is_not_an_event_log_reference():
    with pytest.raises(TypeError, match="Not an event log reference"):
        build_cli_args("sparkforensics-analyze", Path("/tmp/app.log"), {})


def test_build_cli_args_passes_the_baseline_path_and_the_comparison_budgets():
    args = build_cli_args(
        "sparkforensics-analyze",
        HistoryServerApp(base_url="http://localhost:18080", app_id="app-2"),
        {"max_regression_pct": 20, "regression_metric": "gcTime", "fail_on_introduced": "all"},
        baseline_ref=RemoteEventLog("onprem_ssh", "/logs/app-1"),
    )

    assert args == [
        "sparkforensics-analyze", "--shs-base-url", "http://localhost:18080", "--app-id", "app-2",
        "--format", "json", "--baseline", "/logs/app-1",
        "--max-regression-pct", "20", "--regression-metric", "gcTime", "--fail-on-introduced", "all",
    ]


def test_build_cli_args_passes_a_local_baseline_path():
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app-2.log")), {},
        baseline_ref=LocalEventLog(Path("/tmp/app-1.log")),
    )

    assert args[args.index("--baseline") + 1] == "/tmp/app-1.log"


def test_build_cli_args_never_passes_a_history_server_app_as_the_baseline():
    with pytest.raises(TypeError, match="Not a baseline event log path"):
        build_cli_args(
            "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {},
            baseline_ref=HistoryServerApp(base_url="http://shs:18080", app_id="app-1"),
        )
