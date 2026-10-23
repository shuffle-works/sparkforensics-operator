"""
The sparkforensics-analyze CLI contract, shared by every backend whatever
host it runs the CLI on: argument building, exit-code handling, and turning
the JSON report plus stderr threshold lines into a Report.
"""
from __future__ import annotations

import re
from pathlib import PurePath

from sparkforensics_operator._compat import AirflowException
from sparkforensics_operator.exceptions import CliConfigurationError, CliError, HistoryServerFetchError
from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, LocalEventLog, RemoteEventLog
from sparkforensics_operator.report import (
    THRESHOLD_CLI_FLAGS,
    Report,
    parse_report_text,
    parse_threshold_results,
)

# Exit codes 0 (success), 1 (threshold violated), and 3 (thresholds
# inconclusive) per the CLI's documented exit-code contract; anything else
# (OOM-kill, wrapper failure, etc.) means the report is not trustworthy.
USABLE_REPORT_EXIT_CODES = {0, 1, 3}

# The CLI's exit codes that carry no report: what the CLI did, and a short label.
_EXIT_CODE_MEANINGS = {
    2: ("rejected its arguments", "usage error"),
    4: ("could not read or parse the event log, or fetch it from the Spark History Server", "event log"),
    5: ("could not read or parse the baseline", "baseline"),
    6: ("failed with an internal error", "internal error"),
}
# Codes whose message can be a Spark History Server fetch failure: the
# candidate's (4) and the baseline's (5).
_HISTORY_SERVER_EXIT_CODES = {4, 5}
# A usage error's stderr ends with the CLI's usage text.
_USAGE_START = "Usage: sparkforensics-analyze"
_UNKNOWN_OPTION = re.compile(r"Unknown option", re.I)
# The Spark History Server failures the CLI names in its stderr, each with
# what it means and whether a retry can clear it. A "refused or errored"
# answer covers an access problem as well as a server error, which only the
# History Server's own log tells apart, so it counts as retryable.
_HISTORY_SERVER_FAILURES = (
    (
        re.compile(r"SHS fetch failed: upstream-unreachable"),
        "the Spark History Server is unreachable from where the CLI runs",
        True,
    ),
    (
        re.compile(r"SHS fetch failed: application-not-found"),
        "the Spark History Server has no such application or attempt, or has not listed a "
        "run that just finished yet",
        True,
    ),
    (
        re.compile(r"SHS fetch failed: access-or-upstream-failure"),
        "the Spark History Server refused the request or answered with an error "
        "(access, proxy or server error)",
        True,
    ),
    (
        re.compile(r"SHS archive body stalled"),
        "the Spark History Server stopped sending the event log mid-transfer",
        True,
    ),
    (
        re.compile(r"SHS archive exceeds"),
        "the event log is larger than the CLI's download cap "
        "(SPARKFORENSICS_MAX_ARCHIVE_BYTES raises it)",
        False,
    ),
    (
        re.compile(r"Invalid SHS request"),
        "the Spark History Server address, application id or attempt id is invalid",
        False,
    ),
)


def build_cli_args(
    analyze_bin: str,
    log_ref: EventLogRef,
    thresholds: dict,
    out_path: PurePath | None = None,
    baseline_ref: EventLogRef | None = None,
) -> list[str]:
    """Without out_path the CLI writes the JSON report to stdout. baseline_ref,
    if given, is the run to compare against (--baseline), a file path the
    backend has already checked it can read."""
    args = [analyze_bin, *_log_ref_args(log_ref), "--format", "json"]
    if baseline_ref is not None:
        args.extend(["--baseline", _baseline_path(baseline_ref)])
    if out_path is not None:
        args.extend(["--out", str(out_path)])
    args.extend(_threshold_args(thresholds))
    return args


def _log_ref_args(log_ref: EventLogRef) -> list[str]:
    if isinstance(log_ref, HistoryServerApp):
        args = ["--shs-base-url", log_ref.base_url, "--app-id", log_ref.app_id]
        if log_ref.attempt_id:
            args.extend(["--attempt-id", log_ref.attempt_id])
        return args
    if isinstance(log_ref, (LocalEventLog, RemoteEventLog)):
        return [str(log_ref.path)]
    raise TypeError(f"Not an event log reference: {log_ref!r}")


def _baseline_path(baseline_ref: EventLogRef) -> str:
    # --baseline takes only a local event-log file or rolling-log directory;
    # the CLI cannot fetch a baseline from a History Server.
    if isinstance(baseline_ref, (LocalEventLog, RemoteEventLog)):
        return str(baseline_ref.path)
    raise TypeError(f"Not a baseline event log path: {baseline_ref!r}")


def _threshold_args(thresholds: dict) -> list[str]:
    args = []
    for key, flag in THRESHOLD_CLI_FLAGS.items():
        value = thresholds.get(key)
        if value is not None:
            args.extend([flag, str(value)])
    return args


def check_exit_code(returncode: int, stderr: str, where: str = "") -> None:
    """Raise unless returncode means the report can be trusted. where names
    the host the CLI ran on, for backends that run it off the worker, e.g.
    " on the SSH host (ssh_conn_id='onprem_ssh')"."""
    if returncode not in USABLE_REPORT_EXIT_CODES:
        raise cli_error_for_exit_code(returncode, stderr, where)


def cli_error_for_exit_code(returncode: int, stderr: str, where: str = "") -> CliError:
    """The error for a CLI exit code that carries no report. A Spark History
    Server failure is told apart by the CLI's own message: the ones a retry
    can clear raise HistoryServerFetchError, the ones it cannot (and a usage
    error) raise CliConfigurationError, which Airflow does not retry."""
    detail = _cli_message(stderr)
    if returncode not in _EXIT_CODE_MEANINGS:
        return CliError(
            f"sparkforensics-analyze{where} exited with unexpected code {returncode}: {detail}",
            returncode,
        )
    meaning, label = _EXIT_CODE_MEANINGS[returncode]
    problem = f"sparkforensics-analyze{where} {meaning} (exit {returncode}, {label})"
    if returncode in _HISTORY_SERVER_EXIT_CODES:
        failure = _history_server_failure(detail)
        if failure is not None:
            cause, retryable = failure
            error_type = HistoryServerFetchError if retryable else CliConfigurationError
            return error_type(f"{problem}: {cause}. The CLI said: {detail}", returncode)
    message = f"{problem}: {detail}"
    if returncode == 2:
        if _UNKNOWN_OPTION.search(detail):
            message += (
                " The installed sparkforensics-cli does not know this option: upgrade it "
                "(docs/configuration.md lists the version each option needs)."
            )
        return CliConfigurationError(message, returncode)
    return CliError(message, returncode)


def _cli_message(stderr: str) -> str:
    """The CLI's stderr without the usage text it appends to a usage error."""
    return stderr.partition(_USAGE_START)[0].strip()


def _history_server_failure(detail: str) -> tuple[str, bool] | None:
    """(what went wrong, whether a retry can help) for a History Server
    failure the CLI's message names, or None."""
    for pattern, cause, retryable in _HISTORY_SERVER_FAILURES:
        if pattern.search(detail):
            return cause, retryable
    return None


def build_report(
    report_text: str, thresholds: dict, stderr: str, returncode: int, where: str = ""
) -> Report:
    try:
        report = parse_report_text(report_text)
    except (ValueError, KeyError, TypeError) as e:
        raise AirflowException(
            f"sparkforensics-analyze{where} exited {returncode} but its JSON report "
            f"could not be parsed: {e!r}"
        ) from e
    report.threshold_results = parse_threshold_results(thresholds, stderr)
    report.exit_code = returncode
    return report
