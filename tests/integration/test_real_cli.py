"""The operator against the real sparkforensics-analyze CLI and public corpus
logs. The rest of the suite simulates the CLI, so it cannot notice the CLI
changing an exit code or a flag's rules; these tests can. See conftest.py for
what they need and when they skip."""
from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowFailException

from sparkforensics_operator import LocalEventLog, SubprocessAnalyzeHook
from sparkforensics_operator.exceptions import CliError
from sparkforensics_operator.report import MAX_SCHEMA_VERSION, cli_version_warning

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


def test_an_unreadable_log_is_exit_4(backend, tmp_path):
    with pytest.raises(CliError) as raised:
        backend.analyze(LocalEventLog(tmp_path / "missing.ndjson"), {})

    assert raised.value.exit_code == 4
    assert "could not read or parse the event log" in str(raised.value)
    assert "(exit 4, event log)" in str(raised.value)
    assert not isinstance(raised.value, AirflowFailException)


def test_an_unreadable_baseline_is_exit_5(backend, clean_log, tmp_path):
    with pytest.raises(CliError) as raised:
        backend.analyze(
            LocalEventLog(clean_log), {"max_regression_pct": 10},
            baseline_ref=LocalEventLog(tmp_path / "missing-baseline.ndjson"),
        )

    assert raised.value.exit_code == 5
    assert "could not read or parse the baseline (exit 5, baseline)" in str(raised.value)


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
