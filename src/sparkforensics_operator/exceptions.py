from __future__ import annotations

from sparkforensics_operator._compat import AirflowException, AirflowFailException


class ThresholdBreached(AirflowFailException):
    """Raised when a configured SparkForensics threshold is violated and
    on_threshold_breach="fail". Extends AirflowFailException (not
    AirflowException): a breach is a deterministic verdict from the
    already-completed analysis, so retrying would just re-run the same
    fetch+analyze cycle to reach the same answer, and should not consume
    the task's configured retries."""


class CliError(AirflowException):
    """sparkforensics-analyze exited with a code that carries no report."""

    def __init__(self, message: str, exit_code: int | None = None):
        super().__init__(message)
        self.exit_code = exit_code


class CliConfigurationError(CliError, AirflowFailException):
    """The CLI refused its arguments or its Spark History Server settings.
    A retry would fail the same way, so Airflow does not retry it."""


class HistoryServerFetchError(CliError):
    """A Spark History Server failure that can clear on a retry."""
