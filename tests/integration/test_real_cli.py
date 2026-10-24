"""The operator against the real sparkforensics-analyze CLI and public corpus
logs. The rest of the suite simulates the CLI, so it cannot notice the CLI
changing an exit code or a flag's rules; these tests can. See conftest.py for
what they need and when they skip."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException

from sparkforensics_operator import (
    FilesystemLogSourceHook,
    HistoryServerApp,
    LocalEventLog,
    RemoteEventLog,
    SparkForensicsOperator,
    SSHAnalyzeHook,
    SubprocessAnalyzeHook,
    ThresholdBreached,
)
from sparkforensics_operator.exceptions import CliConfigurationError, CliError, HistoryServerFetchError
from sparkforensics_operator.report import MAX_SCHEMA_VERSION, TESTED_CLI_VERSION, cli_version_warning
from sparkforensics_operator.summary import SUMMARY_XCOM_KEY
from tests.hooks.analyze._local_ssh import LocalHost, needs_posix_host, run_trigger, ti_context

pytestmark = pytest.mark.cli_integration

# Budgets small enough that the CLI measures them against a log with task data.
SMALL_BUDGETS = {
    "max_runtime_ms": 3_600_000,
    "max_spill_gb": 0.00001,
    "max_skew_ratio": 0.00001,
    "max_failed_task_rate_pct": 0.00001,
    "min_efficiency_pct": 0.00001,
}


@pytest.fixture
def backend(analyze_bin) -> SubprocessAnalyzeHook:
    return SubprocessAnalyzeHook(analyze_bin=analyze_bin, timeout=120)


def _results(report) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    for result in report.threshold_results:
        results.setdefault(result.name, []).append(result.status)
    return results


def test_a_clean_log_passes_its_budget_and_reports_the_run(backend, clean_log):
    report = backend.analyze(LocalEventLog(clean_log), {"max_runtime_ms": 10_000_000})

    assert report.exit_code in (0, 3)
    assert not report.violated
    assert _results(report)["max-runtime"] == ["pass"]
    assert report.schema_version == MAX_SCHEMA_VERSION
    assert report.app_name == "daily_customer_etl"
    assert report.run_time_ms == 2_832_000
    assert report.verdict_title
    assert cli_version_warning(report.cli_version) is None, report.cli_version
    assert report.raw["generator"]["name"] == "sparkforensics-cli"
    assert "metrics" in report.raw and "effectiveConf" in report.raw


def test_a_budget_the_log_breaks_is_a_violation_with_exit_1(backend, clean_log):
    report = backend.analyze(LocalEventLog(clean_log), {"max_runtime_ms": 1000})

    assert report.exit_code == 1
    assert report.violated
    assert _results(report) == {"max-runtime": ["violation"]}
    assert "exceeds budget 1000ms" in report.threshold_results[0].detail


def test_the_operator_persists_the_whole_cli_report_and_fails_on_a_violation(backend, clean_log, tmp_path):
    dest = tmp_path / "report.json"
    op = SparkForensicsOperator(
        task_id="forensics",
        log_source=FilesystemLogSourceHook(path_template=str(clean_log)),
        backend=backend,
        report_dest=str(dest),
        max_runtime_ms=1000,
    )
    ti = MagicMock()

    with pytest.raises(ThresholdBreached, match="max-runtime: Runtime 2832000ms exceeds budget 1000ms"):
        op.execute({"ti": ti})

    persisted = json.loads(dest.read_text())
    assert {"summary", "verdict", "findings", "metrics", "effectiveConf", "writeTargets", "generator"} <= set(persisted)
    assert persisted["thresholdResults"] == [
        {"name": "max-runtime", "status": "violation", "detail": persisted["thresholdResults"][0]["detail"]}
    ]
    summary = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}[SUMMARY_XCOM_KEY]
    assert summary["violated"] is True
    assert summary["exit_code"] == 1
    assert summary["app_name"] == "daily_customer_etl"
    assert summary["run_time_ms"] == 2_832_000
    assert summary["cli_version"] == persisted["generator"]["version"]
    assert summary["verdict"] == persisted["verdict"]["title"]


def test_a_log_with_a_skipped_line_is_inconclusive_when_the_cli_reports_skipped_lines(backend, clean_log, tmp_path):
    damaged = tmp_path / "damaged.ndjson"
    damaged.write_text(clean_log.read_text() + "this line is not json {\n")

    report = backend.analyze(LocalEventLog(damaged), {"max_runtime_ms": 10_000_000})

    if report.skipped_lines is None:
        pytest.skip("this sparkforensics-cli does not report skipped lines")
    assert report.skipped_lines == 1
    assert report.exit_code == 3
    assert _results(report)["parse-incomplete"] == ["inconclusive"]
    assert not report.violated


def test_an_unreadable_log_is_exit_4(backend, tmp_path):
    with pytest.raises(CliError) as raised:
        backend.analyze(LocalEventLog(tmp_path / "missing.ndjson"), {})

    assert raised.value.exit_code == 4
    assert "could not read or parse the event log" in str(raised.value)
    assert "(exit 4, event log)" in str(raised.value)
    assert not isinstance(raised.value, AirflowFailException)


def test_a_file_that_is_not_an_event_log_is_exit_4(backend, tmp_path):
    garbage = tmp_path / "garbage.ndjson"
    garbage.write_text("this is not an event log\n")

    with pytest.raises(CliError) as raised:
        backend.analyze(LocalEventLog(garbage), {})

    assert raised.value.exit_code == 4


def test_an_unreadable_baseline_is_exit_5(backend, clean_log, tmp_path):
    with pytest.raises(CliError) as raised:
        backend.analyze(
            LocalEventLog(clean_log), {"max_regression_pct": 10},
            baseline_ref=LocalEventLog(tmp_path / "missing-baseline.ndjson"),
        )

    assert raised.value.exit_code == 5
    assert "could not read or parse the baseline (exit 5, baseline)" in str(raised.value)


def test_a_budget_in_a_form_the_cli_refuses_is_a_usage_error_airflow_does_not_retry(backend, clean_log, failed_log):
    with pytest.raises(CliConfigurationError) as raised:
        backend.analyze(
            LocalEventLog(clean_log), {"max_regression_pct": 10, "regression_metric": "notAMetric"},
            baseline_ref=LocalEventLog(failed_log),
        )

    assert raised.value.exit_code == 2
    assert "rejected its arguments (exit 2, usage error)" in str(raised.value)
    assert "notAMetric" in str(raised.value)
    assert "Usage: sparkforensics-analyze" not in str(raised.value)
    assert isinstance(raised.value, AirflowFailException)


def test_budgets_small_enough_to_print_in_exponent_form_reach_the_cli_as_plain_decimals(backend, task_log):
    real_run = subprocess.run
    commands = []

    def spy(args, **kwargs):
        commands.append(list(args))
        return real_run(args, **kwargs)

    with patch("subprocess.run", side_effect=spy):
        report = backend.analyze(LocalEventLog(task_log), SMALL_BUDGETS)

    flags = commands[0]
    assert flags[flags.index("--max-spill") + 1] == "0.00001"
    assert flags[flags.index("--max-skew") + 1] == "0.00001"
    assert not any("e-" in arg or "e+" in arg for arg in flags)
    # The CLI measured them (a usage error would have raised): the log's skew breaks 0.00001.
    assert report.exit_code == 1
    assert _results(report)["max-skew"] == ["violation"]


def test_several_regression_budgets_each_keep_their_own_result(backend, clean_log, failed_log):
    report = backend.analyze(
        LocalEventLog(clean_log),
        {"max_regression_pct": 10, "regression_budgets": {"gcTime": 5, "executorRunTime": 5}},
        baseline_ref=LocalEventLog(failed_log),
    )

    results = [r for r in report.threshold_results if r.name == "max-regression"]
    assert len(results) >= 2
    assert any('Metric "wallClock"' in r.detail for r in results)
    assert any('Metric "gcTime"' in r.detail for r in results)
    assert report.comparison is not None


def test_the_comparison_tuning_flags_and_stage_budgets_are_accepted(backend, task_log, tmp_path):
    other = tmp_path / "other.ndjson"
    other.write_bytes(task_log.read_bytes())

    report = backend.analyze(
        LocalEventLog(task_log),
        {
            "stage_regression_budgets": {"executorRunTime": 50},
            "stage_qualities": ["exact", "structural"],
            "normalize_path": [r"/tmp/run-\d+"],
            "ignore_conf": [r"^spark\.app\."],
            "noise_band_pct": 5,
        },
        baseline_ref=LocalEventLog(other),
    )

    assert report.comparison is not None
    assert report.exit_code in (0, 3)
    assert _results(report)["max-stage-regression"] == ["pass"]


def test_redact_pseudonymizes_the_application(backend, clean_log):
    report = backend.analyze(LocalEventLog(clean_log), {"redact": True})

    assert report.app_name != "daily_customer_etl"
    assert "daily_customer_etl" not in json.dumps(report.raw)


def test_markdown_dest_persists_the_markdown_report_from_a_second_run(backend, clean_log, tmp_path):
    op = SparkForensicsOperator(
        task_id="forensics",
        log_source=FilesystemLogSourceHook(path_template=str(clean_log)),
        backend=backend,
        report_dest=str(tmp_path / "report.json"),
        markdown_dest=str(tmp_path / "report.md"),
    )
    ti = MagicMock()

    op.execute({"ti": ti})

    markdown = (tmp_path / "report.md").read_text()
    assert markdown.startswith("# Spark run evidence report")
    assert "daily_customer_etl" in markdown
    summary = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}[SUMMARY_XCOM_KEY]
    assert summary["markdown_destination"] == str(tmp_path / "report.md")


def test_a_history_server_application_is_a_baseline_and_a_candidate(backend, clean_log, failed_log, history_server):
    history_server.serve("application_1_0001", clean_log)
    history_server.serve("application_1_0002", failed_log)

    from_path = backend.analyze(
        LocalEventLog(clean_log), {"max_regression_pct": 10},
        baseline_ref=HistoryServerApp(history_server.base_url, "application_1_0002"),
    )
    from_server = backend.analyze(
        HistoryServerApp(history_server.base_url, "application_1_0001"), {"max_regression_pct": 10},
        baseline_ref=HistoryServerApp(history_server.base_url, "application_1_0002"),
    )

    assert from_path.comparison is not None and from_server.comparison is not None
    assert from_path.comparison["metrics"] == from_server.comparison["metrics"]
    assert history_server.requests.count("/api/v1/applications/application_1_0002/logs") == 2
    assert _results(from_path) == _results(from_server) == {"max-regression": ["violation"]}


def test_a_history_server_that_has_no_such_application_is_a_fetch_error_a_retry_can_clear(backend, clean_log, history_server):
    with pytest.raises(HistoryServerFetchError) as raised:
        backend.analyze(
            LocalEventLog(clean_log), {"max_regression_pct": 10},
            baseline_ref=HistoryServerApp(history_server.base_url, "application_1_9999"),
        )

    assert raised.value.exit_code == 5
    assert "has no such application or attempt" in str(raised.value)
    assert not isinstance(raised.value, AirflowFailException)


def test_a_history_server_that_is_down_is_a_fetch_error_a_retry_can_clear(backend, closed_port_url):
    with pytest.raises(HistoryServerFetchError) as raised:
        backend.analyze(HistoryServerApp(closed_port_url, "application_1_0001"), {})

    assert raised.value.exit_code == 4
    assert "Spark History Server" in str(raised.value)
    assert not isinstance(raised.value, AirflowFailException)


def test_the_tested_cli_version_is_not_newer_than_the_installed_one(backend, clean_log):
    report = backend.analyze(LocalEventLog(clean_log), {})

    assert cli_version_warning(report.cli_version) is None, (
        f"sparkforensics-cli {report.cli_version} is older than {TESTED_CLI_VERSION}"
    )


@pytest.fixture
def host(tmp_path, analyze_bin):
    """A stand-in SSH host that runs the real CLI."""
    pytest.importorskip("airflow.providers.ssh.hooks.ssh")
    host = LocalHost(tmp_path)
    (host.bin_dir / "sparkforensics-analyze").symlink_to(analyze_bin)
    with host.patched():
        yield host


@needs_posix_host
def test_the_ssh_backend_runs_the_real_cli_on_the_host(host, clean_log, failed_log):
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=120)

    report = hook.analyze(
        RemoteEventLog("onprem_ssh", str(clean_log)), {"max_runtime_ms": 1000, "markdown": True, "redact": True},
    )
    compared = hook.analyze(
        RemoteEventLog("onprem_ssh", str(clean_log)), {"max_regression_pct": 10},
        baseline_ref=RemoteEventLog("onprem_ssh", str(failed_log)),
    )

    assert report.exit_code == 1
    assert _results(report) == {"max-runtime": ["violation"]}
    assert report.markdown.startswith("# Spark run evidence report")
    assert report.app_name != "daily_customer_etl"
    assert compared.comparison is not None
    with pytest.raises(CliError) as raised:
        hook.analyze(RemoteEventLog("onprem_ssh", str(clean_log.parent / "missing")), {})
    assert raised.value.exit_code == 4


@needs_posix_host
def test_the_deferrable_ssh_backend_runs_the_real_cli_as_a_detached_job(host, clean_log):
    pytest.importorskip("airflow.providers.ssh.triggers.ssh_remote_job")
    hook = SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=120, poll_interval=0.2)
    if hook.cannot_defer_reason():
        pytest.skip(hook.cannot_defer_reason())
    log_ref = RemoteEventLog("onprem_ssh", str(clean_log))
    thresholds = {"max_runtime_ms": 1000, "markdown": True}

    job = hook.submit(log_ref, thresholds, ti_context())
    event = run_trigger(hook.trigger_for(job), timeout=120)
    report = hook.collect(job, event, log_ref, thresholds)

    assert event["exit_code"] == 1
    assert report.exit_code == 1
    assert _results(report) == {"max-runtime": ["violation"]}
    assert report.markdown.startswith("# Spark run evidence report")
    assert not Path(job["scope_dir"]).exists()
