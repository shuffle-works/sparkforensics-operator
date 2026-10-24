import json
from decimal import Decimal
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException, AirflowFailException

from sparkforensics_operator.exceptions import CliConfigurationError, CliError, HistoryServerFetchError
from sparkforensics_operator.hooks.analyze._cli import build_cli_args, build_report, check_exit_code
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog

# What the CLI writes to stderr after the message of a usage error.
_USAGE = "Usage: sparkforensics-analyze <event-log-file|rolling-log-dir> [options]\n  --format md|json\n"


def test_build_cli_args_writes_to_stdout_without_an_out_path():
    args = build_cli_args("sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_spill_gb": 2})

    assert args == ["sparkforensics-analyze", "/tmp/app.log", "--format", "json", "--max-spill", "2"]


@pytest.mark.parametrize(
    "value, expected",
    [
        (0.00001, "0.00001"),
        (1e-07, "0.0000001"),
        (1.5e-05, "0.000015"),
        (1e16, "10000000000000000"),
        (2.5e20, "250000000000000000000"),
        (90, "90"),
        (3.0, "3.0"),
        (1.5, "1.5"),
        (0, "0"),
        (Decimal("1E-5"), "0.00001"),
    ],
)
def test_build_cli_args_writes_a_threshold_as_a_plain_decimal(value, expected):
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_spill_gb": value}
    )

    assert args[args.index("--max-spill") + 1] == expected
    assert "e" not in expected.lower()


def test_build_cli_args_writes_every_numeric_threshold_as_a_plain_decimal():
    args = build_cli_args(
        "sparkforensics-analyze",
        LocalEventLog(Path("/tmp/app.log")),
        {
            "max_runtime_ms": 10**16,
            "max_spill_gb": 0.00001,
            "max_skew_ratio": 0.00002,
            "max_failed_task_rate_pct": 0.00003,
            "min_efficiency_pct": 1e-06,
            "max_regression_pct": 0.0000004,
            "regression_metric": "gcTime",
            "fail_on_introduced": "critical",
        },
        baseline_ref=LocalEventLog(Path("/tmp/app-0.log")),
    )

    flags = dict(zip(args[args.index("--format") + 2 :: 2], args[args.index("--format") + 3 :: 2]))
    assert flags["--max-runtime"] == "10000000000000000"
    assert flags["--max-spill"] == "0.00001"
    assert flags["--max-skew"] == "0.00002"
    assert flags["--max-failed-task-rate"] == "0.00003"
    assert flags["--min-efficiency"] == "0.000001"
    assert flags["--max-regression-pct"] == "0.0000004"
    assert flags["--regression-metric"] == "gcTime"
    assert flags["--fail-on-introduced"] == "critical"


def test_build_cli_args_passes_a_string_threshold_as_it_is():
    args = build_cli_args("sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_spill_gb": "2"})

    assert args[args.index("--max-spill") + 1] == "2"


def test_build_cli_args_repeats_a_regression_budget_flag_per_metric():
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app-2.log")),
        {
            "max_regression_pct": 20,
            "regression_budgets": {"gcTime": 10, "diskSpill": 0.00001},
            "stage_regression_budgets": {"executorRunTime": 30, "shuffleReadBytes": 1e16},
            "stage_qualities": ["exact", "structural"],
        },
        baseline_ref=LocalEventLog(Path("/tmp/app-1.log")),
    )

    assert args == [
        "sparkforensics-analyze", "/tmp/app-2.log", "--format", "json", "--baseline", "/tmp/app-1.log",
        "--max-regression-pct", "20", "--stage-quality", "exact,structural",
        "--regression-budget", "gcTime:10", "--regression-budget", "diskSpill:0.00001",
        "--stage-regression-budget", "executorRunTime:30",
        "--stage-regression-budget", "shuffleReadBytes:10000000000000000",
    ]


def test_build_cli_args_passes_the_comparison_tuning_flags():
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app-2.log")),
        {
            "normalize_path": ["/out/run-\\d+", "tmp[0-9a-f]+"],
            "ignore_conf": ["^spark\\.app\\.", "output"],
            "noise_band_pct": 0.5,
        },
        baseline_ref=LocalEventLog(Path("/tmp/app-1.log")),
    )

    assert args == [
        "sparkforensics-analyze", "/tmp/app-2.log", "--format", "json", "--baseline", "/tmp/app-1.log",
        "--noise-band-pct", "0.5",
        "--normalize-path", "/out/run-\\d+", "--normalize-path", "tmp[0-9a-f]+",
        "--ignore-conf", "^spark\\.app\\.", "--ignore-conf", "output",
    ]


def test_build_cli_args_writes_a_small_noise_band_as_a_plain_decimal():
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"noise_band_pct": 0.00001},
        baseline_ref=LocalEventLog(Path("/tmp/app-1.log")),
    )

    assert args[args.index("--noise-band-pct") + 1] == "0.00001"


@pytest.mark.parametrize("empty", [None, {}, []])
def test_build_cli_args_passes_no_budget_flag_for_an_empty_budget(empty):
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")),
        {
            "regression_budgets": empty, "stage_regression_budgets": empty, "stage_qualities": empty,
            "normalize_path": empty, "ignore_conf": empty,
        },
    )

    assert args == ["sparkforensics-analyze", "/tmp/app.log", "--format", "json"]


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


def test_build_cli_args_passes_a_history_server_app_as_the_baseline():
    args = build_cli_args(
        "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_regression_pct": 10},
        baseline_ref=HistoryServerApp(base_url="http://shs:18080", app_id="app-1"),
    )

    assert args == [
        "sparkforensics-analyze", "/tmp/app.log", "--format", "json",
        "--baseline-shs-base-url", "http://shs:18080", "--baseline-app-id", "app-1",
        "--max-regression-pct", "10",
    ]
    assert "--baseline" not in args


def test_build_cli_args_passes_the_baseline_attempt_id_when_there_is_one():
    args = build_cli_args(
        "sparkforensics-analyze", HistoryServerApp(base_url="http://shs:18080", app_id="app-2", attempt_id="2"), {},
        baseline_ref=HistoryServerApp(base_url="http://other-shs:18080", app_id="app-1", attempt_id="3"),
    )

    assert args == [
        "sparkforensics-analyze", "--shs-base-url", "http://shs:18080", "--app-id", "app-2",
        "--attempt-id", "2", "--format", "json",
        "--baseline-shs-base-url", "http://other-shs:18080", "--baseline-app-id", "app-1",
        "--baseline-attempt-id", "3",
    ]


def test_build_cli_args_rejects_a_baseline_that_is_not_an_event_log_reference():
    with pytest.raises(TypeError, match="Not a baseline event log reference"):
        build_cli_args(
            "sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {}, baseline_ref=Path("/tmp/app-1.log"),
        )


@pytest.mark.parametrize("returncode", [0, 1, 3])
def test_check_exit_code_accepts_the_codes_that_carry_a_report(returncode):
    check_exit_code(returncode, "[violation] max-runtime: Runtime 1ms exceeds budget 0ms.\n")


def test_exit_2_is_a_usage_error_that_airflow_does_not_retry():
    stderr = '--max-spill needs a non-negative decimal number such as 90 or 1.5, got "-1".\n' + _USAGE

    with pytest.raises(CliConfigurationError) as raised:
        check_exit_code(2, stderr)

    message = str(raised.value)
    assert "rejected its arguments (exit 2, usage error)" in message
    assert '--max-spill needs a non-negative decimal number such as 90 or 1.5, got "-1".' in message
    assert "Usage: sparkforensics-analyze" not in message
    assert raised.value.exit_code == 2
    assert isinstance(raised.value, AirflowFailException)


def test_exit_2_for_an_option_the_cli_does_not_know_names_the_upgrade():
    stderr = "Unknown option '--ignore-conf'. To specify a positional argument starting with a '-', ..." + _USAGE

    with pytest.raises(CliConfigurationError, match="does not know this option: upgrade it"):
        check_exit_code(2, stderr, where=" on the SSH host (ssh_conn_id='onprem_ssh')")


def test_exit_4_is_an_unreadable_event_log_that_airflow_retries():
    with pytest.raises(CliError) as raised:
        check_exit_code(4, "ENOENT: no such file or directory, stat '/logs/app-1'\n")

    message = str(raised.value)
    assert "could not read or parse the event log" in message
    assert "(exit 4, event log)" in message
    assert "ENOENT: no such file or directory" in message
    assert raised.value.exit_code == 4
    assert not isinstance(raised.value, (AirflowFailException, HistoryServerFetchError))


@pytest.mark.parametrize(
    "stderr, cause",
    [
        ("SHS fetch failed: upstream-unreachable\n", "is unreachable from where the CLI runs"),
        ("SHS fetch failed: application-not-found\n", "has no such application or attempt"),
        ("SHS fetch failed: access-or-upstream-failure\n", "refused the request or answered with an error"),
        (
            "SHS archive body stalled for 30000 ms (override with SPARKFORENSICS_SHS_TIMEOUT_MS).\n",
            "stopped sending the event log mid-transfer",
        ),
    ],
    ids=["unreachable", "not-found", "refused-or-errored", "stalled"],
)
def test_a_history_server_failure_a_retry_can_clear_is_a_fetch_error(stderr, cause):
    with pytest.raises(HistoryServerFetchError) as raised:
        check_exit_code(4, stderr)

    assert cause in str(raised.value)
    assert f"The CLI said: {stderr.strip()}" in str(raised.value)
    assert not isinstance(raised.value, AirflowFailException)


@pytest.mark.parametrize(
    "stderr, cause",
    [
        (
            "SHS archive exceeds the 1073741824-byte cap (override with SPARKFORENSICS_MAX_ARCHIVE_BYTES).\n",
            "larger than the CLI's download cap",
        ),
        ("Invalid SHS request parameters.\n", "address, application id or attempt id is invalid"),
    ],
    ids=["archive-too-large", "invalid-request"],
)
def test_a_history_server_failure_a_retry_cannot_clear_is_a_configuration_error(stderr, cause):
    with pytest.raises(CliConfigurationError) as raised:
        check_exit_code(4, stderr)

    assert cause in str(raised.value)
    assert isinstance(raised.value, AirflowFailException)


def test_exit_5_is_an_unreadable_baseline():
    with pytest.raises(CliError) as raised:
        check_exit_code(5, "ENOENT: no such file or directory, stat '/logs/app-0'\n")

    assert "could not read or parse the baseline (exit 5, baseline)" in str(raised.value)
    assert raised.value.exit_code == 5


def test_exit_5_from_a_baseline_history_server_fetch_is_classified_like_exit_4():
    with pytest.raises(HistoryServerFetchError, match="could not read or parse the baseline"):
        check_exit_code(5, "SHS fetch failed: upstream-unreachable\n")


def test_exit_6_is_an_internal_error_with_the_clis_stack():
    with pytest.raises(CliError) as raised:
        check_exit_code(6, "Internal error: Error: boom\n    at run (file:///cli.mjs:1:1)\n")

    assert "failed with an internal error (exit 6, internal error)" in str(raised.value)
    assert "Error: boom" in str(raised.value)
    assert raised.value.exit_code == 6


def test_an_exit_code_the_cli_does_not_define_is_unexpected():
    with pytest.raises(CliError, match="exited with unexpected code 137: Killed"):
        check_exit_code(137, "Killed\n")


def test_every_cli_error_is_an_airflow_exception():
    for error_type in (CliError, CliConfigurationError, HistoryServerFetchError):
        assert issubclass(error_type, AirflowException)


def test_a_cli_error_keeps_its_exit_code_when_pickled():
    import pickle

    restored = pickle.loads(pickle.dumps(CliConfigurationError("bad arguments", 2)))

    assert str(restored) == "bad arguments"
    assert restored.exit_code == 2


def test_a_report_in_a_newer_schema_than_the_operator_reads_is_a_configuration_error():
    report_text = json.dumps({"schemaVersion": 99})

    with pytest.raises(CliConfigurationError) as raised:
        build_report(report_text, {}, "", 0, where=" on the SSH host (ssh_conn_id='onprem_ssh')")

    message = str(raised.value)
    assert "on the SSH host (ssh_conn_id='onprem_ssh') wrote a report this sparkforensics-operator cannot read" in message
    assert "schemaVersion 99 is newer than this sparkforensics-operator reads" in message
    assert isinstance(raised.value, AirflowFailException)
    assert raised.value.exit_code == 0


def test_a_report_in_an_older_schema_than_the_operator_reads_is_a_configuration_error():
    with pytest.raises(CliConfigurationError, match="requires sparkforensics-cli 0.4.0 or newer"):
        build_report(json.dumps({"schemaVersion": 4}), {}, "", 0)
