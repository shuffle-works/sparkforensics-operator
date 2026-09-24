"""
The sparkforensics-analyze CLI contract, shared by every backend whatever
host it runs the CLI on: argument building, exit-code handling, and turning
the JSON report plus stderr threshold lines into a Report.
"""
from __future__ import annotations

from pathlib import Path

from airflow.exceptions import AirflowException

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


def build_cli_args(
    analyze_bin: str,
    log_ref: EventLogRef,
    thresholds: dict,
    out_path: Path | None = None,
) -> list[str]:
    """Without out_path the CLI writes the JSON report to stdout."""
    args = [analyze_bin, *_log_ref_args(log_ref), "--format", "json"]
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
    if returncode == 2:
        raise AirflowException(
            f"sparkforensics-analyze{where} failed to parse the event log, could not "
            f"fetch it from the Spark History Server, or was given bad arguments "
            f"(exit 2): {stderr.strip()}"
        )
    if returncode not in USABLE_REPORT_EXIT_CODES:
        raise AirflowException(
            f"sparkforensics-analyze{where} exited with unexpected code {returncode}: "
            f"{stderr.strip()}"
        )


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
