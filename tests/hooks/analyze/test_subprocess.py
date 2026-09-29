import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.subprocess import SubprocessAnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog

SAMPLE_JSON = {
    "schemaVersion": 5,
    "summary": {"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
    "verdict": {"title": "No issues found", "summary": [], "steps": [], "remainingPlaces": 0, "copyText": None},
    "evidenceAvailability": None,
    "detectors": [],
    "findings": [],
    "recommendations": [],
    "cleanChecks": [],
    "notRunChecks": [],
}


def _out_path_from_args(args: list) -> Path:
    return Path(args[args.index("--out") + 1])


def _fake_run(returncode: int, stderr: str = ""):
    def run(args, capture_output, text, timeout):
        _out_path_from_args(args).write_text(json.dumps(SAMPLE_JSON))
        return subprocess.CompletedProcess(args=args, returncode=returncode, stdout="", stderr=stderr)
    return run


def test_analyze_builds_cli_args_and_parses_the_out_file(tmp_path):
    hook = SubprocessAnalyzeHook(analyze_bin="sparkforensics-analyze")
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")
    captured_args = {}

    def run(args, capture_output, text, timeout):
        captured_args["args"] = args
        _out_path_from_args(args).write_text(json.dumps(SAMPLE_JSON))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    with patch("subprocess.run", side_effect=run):
        report = hook.analyze(LocalEventLog(log_path), {"max_runtime_ms": 10_000, "max_skew_ratio": 3.0})

    args = captured_args["args"]
    assert args[0] == "sparkforensics-analyze"
    assert args[1] == str(log_path)
    assert "--format" in args and args[args.index("--format") + 1] == "json"
    assert "--max-runtime" in args and args[args.index("--max-runtime") + 1] == "10000"
    assert "--max-skew" in args and args[args.index("--max-skew") + 1] == "3.0"
    assert report.schema_version == 5
    assert report.exit_code == 0


def test_analyze_parses_threshold_results_from_stderr(tmp_path):
    hook = SubprocessAnalyzeHook()
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")
    stderr = "[violation] max-runtime: Runtime 12000ms exceeds budget 10000ms.\n"

    with patch("subprocess.run", side_effect=_fake_run(returncode=1, stderr=stderr)):
        report = hook.analyze(LocalEventLog(log_path), {"max_runtime_ms": 10_000})

    assert report.violated
    assert report.threshold_results[0].name == "max-runtime"
    assert report.exit_code == 1


def test_analyze_raises_on_exit_code_2(tmp_path):
    hook = SubprocessAnalyzeHook()
    log_path = tmp_path / "app.log"
    log_path.write_text("not a spark event log")

    with patch("subprocess.run", side_effect=_fake_run(returncode=2, stderr="Unrecognized event log format")):
        with pytest.raises(AirflowException, match="Unrecognized event log format"):
            hook.analyze(LocalEventLog(log_path), {})


def test_analyze_raises_a_clear_error_when_the_binary_is_missing(tmp_path):
    hook = SubprocessAnalyzeHook(analyze_bin="sparkforensics-analyze")
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")

    with patch("subprocess.run", side_effect=FileNotFoundError()):
        with pytest.raises(AirflowException, match="sparkforensics-analyze"):
            hook.analyze(LocalEventLog(log_path), {})


def test_analyze_raises_a_clear_error_on_timeout(tmp_path):
    hook = SubprocessAnalyzeHook(timeout=5)
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")

    def run(args, capture_output, text, timeout):
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    with patch("subprocess.run", side_effect=run):
        with pytest.raises(AirflowException, match="timed out after 5s"):
            hook.analyze(LocalEventLog(log_path), {})


def test_analyze_cleans_up_the_temp_out_file_on_timeout(tmp_path):
    hook = SubprocessAnalyzeHook(timeout=5)
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")
    captured = {}

    def run(args, capture_output, text, timeout):
        captured["out_path"] = _out_path_from_args(args)
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    with patch("subprocess.run", side_effect=run):
        with pytest.raises(AirflowException):
            hook.analyze(LocalEventLog(log_path), {})

    assert not captured["out_path"].exists()


def test_analyze_raises_on_an_unexpected_exit_code(tmp_path):
    hook = SubprocessAnalyzeHook()
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")

    with patch("subprocess.run", side_effect=_fake_run(returncode=-9, stderr="Killed (OOM)")):
        with pytest.raises(AirflowException, match="Killed \\(OOM\\)"):
            hook.analyze(LocalEventLog(log_path), {})


def test_analyze_cleans_up_the_temp_out_file_on_exit_code_2(tmp_path):
    hook = SubprocessAnalyzeHook()
    log_path = tmp_path / "app.log"
    log_path.write_text("not a spark event log")
    captured = {}

    def run(args, capture_output, text, timeout):
        out_path = _out_path_from_args(args)
        captured["out_path"] = out_path
        out_path.write_text(json.dumps(SAMPLE_JSON))
        return subprocess.CompletedProcess(args=args, returncode=2, stdout="", stderr="bad log")

    with patch("subprocess.run", side_effect=run):
        with pytest.raises(AirflowException):
            hook.analyze(LocalEventLog(log_path), {})

    assert not captured["out_path"].exists()


def test_analyze_cleans_up_the_temp_out_file_on_an_unexpected_exit_code(tmp_path):
    hook = SubprocessAnalyzeHook()
    log_path = tmp_path / "app.log"
    log_path.write_text("{}")
    captured = {}

    def run(args, capture_output, text, timeout):
        out_path = _out_path_from_args(args)
        captured["out_path"] = out_path
        out_path.write_text(json.dumps(SAMPLE_JSON))
        return subprocess.CompletedProcess(args=args, returncode=-9, stdout="", stderr="Killed (OOM)")

    with patch("subprocess.run", side_effect=run):
        with pytest.raises(AirflowException):
            hook.analyze(LocalEventLog(log_path), {})

    assert not captured["out_path"].exists()


def test_analyze_passes_a_history_server_app_to_the_cli_instead_of_a_path():
    hook = SubprocessAnalyzeHook()
    captured_args = {}

    def run(args, capture_output, text, timeout):
        captured_args["args"] = args
        _out_path_from_args(args).write_text(json.dumps(SAMPLE_JSON))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    log_ref = HistoryServerApp(base_url="http://shs:18080", app_id="app-1", attempt_id="2")
    with patch("subprocess.run", side_effect=run):
        report = hook.analyze(log_ref, {})

    args = captured_args["args"]
    assert args[:7] == [
        "sparkforensics-analyze", "--shs-base-url", "http://shs:18080", "--app-id", "app-1",
        "--attempt-id", "2",
    ]
    assert report.schema_version == 5


def test_analyze_rejects_a_log_that_lives_on_an_ssh_host():
    hook = SubprocessAnalyzeHook()

    with patch("subprocess.run") as run:
        with pytest.raises(AirflowException, match="SubprocessAnalyzeHook cannot analyze"):
            hook.analyze(RemoteEventLog(ssh_conn_id="onprem_ssh", path="/logs/app-1"), {})

    run.assert_not_called()


def test_analyze_raises_a_clear_error_when_the_report_is_not_valid_json(tmp_path):
    hook = SubprocessAnalyzeHook()

    def run(args, capture_output, text, timeout):
        _out_path_from_args(args).write_text("not json")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    with patch("subprocess.run", side_effect=run):
        with pytest.raises(AirflowException, match="JSON report could not be parsed"):
            hook.analyze(LocalEventLog(tmp_path / "app.log"), {})


def test_analyze_passes_a_local_baseline_and_reads_the_comparison(tmp_path):
    hook = SubprocessAnalyzeHook()
    comparison = {"confidence": "ok", "reason": None, "metrics": [], "findings": {}}
    captured = {}

    def run(args, capture_output, text, timeout):
        captured["args"] = args
        _out_path_from_args(args).write_text(json.dumps({"candidate": SAMPLE_JSON, "comparison": comparison}))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    with patch("subprocess.run", side_effect=run):
        report = hook.analyze(
            LocalEventLog(tmp_path / "app-2.log"), {"max_regression_pct": 20},
            baseline_ref=LocalEventLog(tmp_path / "app-1.log"),
        )

    args = captured["args"]
    assert args[args.index("--baseline") + 1] == str(tmp_path / "app-1.log")
    assert args[args.index("--max-regression-pct") + 1] == "20"
    assert report.comparison == comparison
    assert [r.name for r in report.threshold_results] == ["max-regression"]


@pytest.mark.parametrize("baseline", [
    HistoryServerApp(base_url="http://shs:18080", app_id="app-1"),
    RemoteEventLog("onprem_ssh", "/logs/app-1"),
])
def test_analyze_rejects_a_baseline_that_is_not_a_worker_local_path_before_running(tmp_path, baseline):
    with patch("subprocess.run") as run:
        with pytest.raises(AirflowException, match="SubprocessAnalyzeHook cannot use"):
            SubprocessAnalyzeHook().analyze(LocalEventLog(tmp_path / "app-2.log"), {}, baseline_ref=baseline)
    run.assert_not_called()
