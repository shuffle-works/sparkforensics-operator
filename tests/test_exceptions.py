from airflow.exceptions import AirflowException, AirflowFailException

from sparkforensics_operator.exceptions import ThresholdBreached


def test_threshold_breached_is_an_airflow_fail_exception():
    assert issubclass(ThresholdBreached, AirflowFailException)
    # AirflowFailException is itself an AirflowException; a breach still
    # fails the task, it just skips retries (see the class docstring).
    assert issubclass(ThresholdBreached, AirflowException)


def test_threshold_breached_carries_its_message():
    err = ThresholdBreached("max-runtime: Runtime 12000ms exceeds budget 10000ms.")
    assert "max-runtime" in str(err)
