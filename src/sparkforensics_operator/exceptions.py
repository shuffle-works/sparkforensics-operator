from airflow.exceptions import AirflowFailException


class ThresholdBreached(AirflowFailException):
    """Raised when a configured SparkForensics threshold is violated and
    on_threshold_breach="fail". Extends AirflowFailException (not
    AirflowException): a breach is a deterministic verdict from the
    already-completed analysis, so retrying would just re-run the same
    fetch+analyze cycle to reach the same answer, and should not consume
    the task's configured retries."""
