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
    """sparkforensics-analyze exited with a code that carries no report.
    exit_code is that code. Airflow retries it like any other task failure;
    the subclasses say when a retry cannot help or is likely to."""

    def __init__(self, message: str, exit_code: int | None = None):
        super().__init__(message)
        self.exit_code = exit_code


class CliConfigurationError(CliError, AirflowFailException):
    """The CLI refused its arguments or its Spark History Server settings.
    Extends AirflowFailException: the same settings fail the same way on
    every try, so a retry would only repeat the failure."""


class HistoryServerFetchError(CliError):
    """The CLI could not fetch a run from a Spark History Server for a
    reason that can clear on its own (the server is unreachable or
    answering with errors, or does not list a run that just finished
    yet), so Airflow's retries apply."""
