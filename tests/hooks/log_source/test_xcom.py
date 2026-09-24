from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.xcom import XComLogSourceHook


def test_resolve_returns_the_xcom_pulled_path(tmp_path):
    log_file = tmp_path / "app-1.log"
    log_file.write_text("{}")
    ti = MagicMock()
    ti.xcom_pull.return_value = str(log_file)
    hook = XComLogSourceHook(task_id="run_spark_job")

    result = hook.resolve({"ti": ti}).path

    ti.xcom_pull.assert_called_once_with(task_ids="run_spark_job", key="return_value")
    assert result == log_file


def test_resolve_raises_when_no_xcom_value_was_pushed():
    ti = MagicMock()
    ti.xcom_pull.return_value = None
    hook = XComLogSourceHook(task_id="run_spark_job")

    with pytest.raises(AirflowException, match="No XCom value"):
        hook.resolve({"ti": ti}).path


def test_resolve_raises_when_the_xcom_path_does_not_exist(tmp_path):
    ti = MagicMock()
    ti.xcom_pull.return_value = str(tmp_path / "missing.log")
    hook = XComLogSourceHook(task_id="run_spark_job")

    with pytest.raises(AirflowException, match="does not exist"):
        hook.resolve({"ti": ti}).path


def test_resolve_uses_a_custom_xcom_key():
    ti = MagicMock()
    ti.xcom_pull.return_value = None
    hook = XComLogSourceHook(task_id="run_spark_job", xcom_key="event_log_path")

    with pytest.raises(AirflowException):
        hook.resolve({"ti": ti}).path

    ti.xcom_pull.assert_called_once_with(task_ids="run_spark_job", key="event_log_path")
