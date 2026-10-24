from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from sparkforensics_operator._compat import AirflowException
from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, LocalEventLog
from sparkforensics_operator.report import Report

from ._cli import USABLE_REPORT_EXIT_CODES, build_cli_args, build_report, check_exit_code
from .base import AnalyzeHook


class SubprocessAnalyzeHook(AnalyzeHook):
    """Shells out to `sparkforensics-analyze <log_path> --format json --out
    <tmpfile> [threshold flags]` on the Airflow worker, or, for a
    HistoryServerApp, `--shs-base-url <url> --app-id <id>` so the CLI
    fetches the run itself. A baseline, when given, is a LocalEventLog
    passed as --baseline, or a HistoryServerApp passed as
    `--baseline-shs-base-url <url> --baseline-app-id <id>`. When the
    Markdown report is wanted, a second run with `--format md` writes it.
    Requires Node.js 18+ and the sparkforensics-cli npm package
    (`npm install -g sparkforensics-cli`) installed on the worker, with
    sparkforensics-analyze resolvable on PATH (or pass analyze_bin=<full
    path>)."""

    supported_log_refs = (LocalEventLog, HistoryServerApp)
    supported_baseline_refs = (LocalEventLog, HistoryServerApp)
    supports_markdown = True

    template_fields = ("analyze_bin",)

    def __init__(self, analyze_bin: str = "sparkforensics-analyze", timeout: int = 900):
        super().__init__()
        self.analyze_bin = analyze_bin
        self.timeout = timeout

    def _analyze(
        self, log_ref: EventLogRef, thresholds: dict, baseline_ref: EventLogRef | None = None
    ) -> Report:
        returncode, report_text, stderr = self._run(log_ref, thresholds, baseline_ref)
        check_exit_code(returncode, stderr)
        report = build_report(report_text, thresholds, stderr, returncode)
        if thresholds.get("markdown"):
            # The CLI writes one format per run, so the Markdown report is a
            # second run over the same log.
            returncode, markdown, stderr = self._run(log_ref, thresholds, baseline_ref, markdown=True)
            check_exit_code(returncode, stderr, " (Markdown report)")
            report.markdown = markdown
        return report

    def _run(
        self,
        log_ref: EventLogRef,
        thresholds: dict,
        baseline_ref: EventLogRef | None,
        markdown: bool = False,
    ) -> tuple[int, str, str]:
        """Runs the CLI with its report written to a temporary file and
        returns (exit code, report text, stderr). The report text is read
        only for an exit code that carries a report."""
        with tempfile.NamedTemporaryFile(suffix=".out", delete=False) as out_file:
            out_path = Path(out_file.name)

        args = build_cli_args(
            self.analyze_bin, log_ref, thresholds,
            out_path=out_path, baseline_ref=baseline_ref, markdown=markdown,
        )

        try:
            try:
                proc = subprocess.run(args, capture_output=True, text=True, timeout=self.timeout)
            except FileNotFoundError as e:
                raise AirflowException(
                    f"sparkforensics-analyze binary not found: {self.analyze_bin!r}. "
                    "Install it with `npm install -g sparkforensics-cli` on this worker, or pass "
                    "analyze_bin=<full path to sparkforensics-analyze>."
                ) from e
            except subprocess.TimeoutExpired as e:
                raise AirflowException(
                    f"sparkforensics-analyze timed out after {self.timeout}s analyzing "
                    f"{log_ref.describe()}."
                ) from e

            report_text = out_path.read_text() if proc.returncode in USABLE_REPORT_EXIT_CODES else ""
            return proc.returncode, report_text, proc.stderr
        finally:
            out_path.unlink(missing_ok=True)
