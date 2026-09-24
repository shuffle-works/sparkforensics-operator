import logging
from unittest.mock import MagicMock

import pytest

from sparkforensics_operator import links
from sparkforensics_operator.links import ReportLink


@pytest.fixture
def airflow_2(monkeypatch):
    monkeypatch.setattr(links, "AIRFLOW_V3_PLUS", False)


@pytest.fixture
def airflow_3(monkeypatch):
    monkeypatch.setattr(links, "AIRFLOW_V3_PLUS", True)


@pytest.fixture
def fake_xcom(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("airflow.models.xcom.XCom", fake)
    return fake


def test_get_link_returns_the_xcom_return_value_on_airflow_2(airflow_2, fake_xcom):
    ti_key = MagicMock()
    fake_xcom.get_value.return_value = "s3://reports/app-1.json"

    result = ReportLink().get_link(MagicMock(), ti_key=ti_key)

    assert result == "s3://reports/app-1.json"
    fake_xcom.get_value.assert_called_once_with(ti_key=ti_key, key="return_value")


def test_get_link_returns_empty_string_when_no_xcom_value_exists_on_airflow_2(airflow_2, fake_xcom):
    fake_xcom.get_value.return_value = None

    assert ReportLink().get_link(MagicMock(), ti_key=MagicMock()) == ""


def test_get_link_logs_the_error_and_returns_empty_string_when_xcom_read_fails_on_airflow_2(
    airflow_2, fake_xcom, caplog
):
    fake_xcom.get_value.side_effect = RuntimeError("db unavailable")

    with caplog.at_level(logging.ERROR, logger="sparkforensics_operator.links"):
        result = ReportLink().get_link(MagicMock(), ti_key=MagicMock())

    assert result == ""
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "db unavailable" in str(record.exc_info[1])


def test_get_link_returns_the_rendered_report_dest_without_reading_xcom_on_airflow_3(airflow_3, fake_xcom):
    operator = MagicMock(report_dest="s3://reports/run-1/report.json")

    result = ReportLink().get_link(operator, ti_key=MagicMock())

    assert result == "s3://reports/run-1/report.json"
    fake_xcom.get_value.assert_not_called()


def test_get_link_propagates_errors_to_the_task_runner_on_airflow_3(airflow_3):
    # Airflow 3's task runner wraps get_link in its own try/except and logs
    # the exception in the task log, so ReportLink must not hide it.
    operator = object()  # no report_dest attribute

    with pytest.raises(AttributeError):
        ReportLink().get_link(operator, ti_key=MagicMock())
