import subprocess
import tempfile
from pathlib import Path

from airflow.exceptions import AirflowException

from sparkforensics_operator.report import THRESHOLD_CLI_FLAGS, Report, parse_report_json, parse_threshold_results

from .base import AnalyzeHook

# Exit codes 0 (success), 1 (threshold violated), and 3 (thresholds inconclusive) per the CLI's documented exit-code contract; anything else (OOM-kill, wrapper failure, etc.) means --out file is not trustworthy.
_USABLE_OUT_FILE_EXIT_CODES = {0, 1, 3}


class SubprocessAnalyzeHook(AnalyzeHook):
    """Shells out to `sparkforensics-analyze <log_path> --format json --out
    <tmpfile> [threshold flags]`. Requires Node.js 18+ and the sparkforensics-cli npm package
    (`npm install -g sparkforensics-cli`) installed on the worker, with sparkforensics-analyze resolvable on
    PATH (or pass analyze_bin=<full path>)."""

    def __init__(self, analyze_bin: str = "sparkforensics-analyze", timeout: int = 900):
        super().__init__()
        self.analyze_bin = analyze_bin
        self.timeout = timeout

    def _threshold_args(self, thresholds: dict) -> list:
        args = []
        for key, flag in THRESHOLD_CLI_FLAGS.items():
            value = thresholds.get(key)
            if value is not None:
                args.extend([flag, str(value)])
        return args

    def analyze(self, log_path: Path, thresholds: dict) -> Report:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as out_file:
            out_path = Path(out_file.name)

        args = [self.analyze_bin, str(log_path), "--format", "json", "--out", str(out_path)]
        args.extend(self._threshold_args(thresholds))

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
                    f"sparkforensics-analyze timed out after {self.timeout}s analyzing {log_path}."
                ) from e

            if proc.returncode == 2:
                raise AirflowException(
                    f"sparkforensics-analyze failed to parse the event log or was given "
                    f"bad arguments (exit 2): {proc.stderr.strip()}"
                )
            if proc.returncode not in _USABLE_OUT_FILE_EXIT_CODES:
                raise AirflowException(
                    f"sparkforensics-analyze exited with unexpected code {proc.returncode}: "
                    f"{proc.stderr.strip()}"
                )

            report = parse_report_json(out_path)
            report.threshold_results = parse_threshold_results(thresholds, proc.stderr)
            report.exit_code = proc.returncode
            return report
        finally:
            out_path.unlink(missing_ok=True)
