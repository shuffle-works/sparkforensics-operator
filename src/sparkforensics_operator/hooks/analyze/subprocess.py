import subprocess
import tempfile
from pathlib import Path

from airflow.exceptions import AirflowException

from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, LocalEventLog
from sparkforensics_operator.report import Report

from ._cli import build_cli_args, build_report, check_exit_code
from .base import AnalyzeHook


class SubprocessAnalyzeHook(AnalyzeHook):
    """Shells out to `sparkforensics-analyze <log_path> --format json --out
    <tmpfile> [threshold flags]` on the Airflow worker, or, for a
    HistoryServerApp, `--shs-base-url <url> --app-id <id>` so the CLI
    fetches the run itself. Requires Node.js 18+ and the sparkforensics-cli
    npm package (`npm install -g sparkforensics-cli`) installed on the
    worker, with sparkforensics-analyze resolvable on PATH (or pass
    analyze_bin=<full path>)."""

    supported_log_refs = (LocalEventLog, HistoryServerApp)

    def __init__(self, analyze_bin: str = "sparkforensics-analyze", timeout: int = 900):
        super().__init__()
        self.analyze_bin = analyze_bin
        self.timeout = timeout

    def _analyze(self, log_ref: EventLogRef, thresholds: dict) -> Report:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as out_file:
            out_path = Path(out_file.name)

        args = build_cli_args(self.analyze_bin, log_ref, thresholds, out_path=out_path)

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

            check_exit_code(proc.returncode, proc.stderr)
            return build_report(out_path.read_text(), thresholds, proc.stderr, proc.returncode)
        finally:
            out_path.unlink(missing_ok=True)
