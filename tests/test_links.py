import logging
from unittest.mock import MagicMock

import pytest

from sparkforensics_operator import links
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.operator import SparkForensicsOperator
from sparkforensics_operator.report import Report, ThresholdResult


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


def _operator(tmp_path, report, **kwargs):
    log_source = MagicMock()
    log_source.fetch.return_value = tmp_path / "app.log"
    backend = MagicMock()
    backend.analyze.return_value = report
    return SparkForensicsOperator(
        task_id="run_forensics", log_source=log_source, backend=backend,
        report_dest=str(tmp_path / "report.json"), **kwargs,
    )


def _report(*threshold_results):
    return Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[], threshold_results=list(threshold_results),
        exit_code=0,
    )


def test_get_link_returns_the_persisted_destination_after_a_successful_run_on_airflow_3(
    airflow_3, fake_xcom, tmp_path
):
    op = _operator(tmp_path, _report())
    destination = op.execute({})

    result = ReportLink().get_link(op, ti_key=MagicMock())

    assert result == destination == str(tmp_path / "report.json")
    fake_xcom.get_value.assert_not_called()


def test_get_link_returns_the_persisted_destination_after_a_threshold_breach_on_airflow_3(
    airflow_3, tmp_path
):
    op = _operator(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
        max_runtime_ms=1000,
    )
    with pytest.raises(ThresholdBreached):
        op.execute({})

    assert ReportLink().get_link(op, ti_key=MagicMock()) == str(tmp_path / "report.json")


def test_get_link_returns_empty_string_when_the_run_failed_before_persisting_on_airflow_3(
    airflow_3, tmp_path
):
    op = _operator(tmp_path, _report())
    op.backend.analyze.side_effect = RuntimeError("analyze failed")
    with pytest.raises(RuntimeError):
        op.execute({})

    assert ReportLink().get_link(op, ti_key=MagicMock()) == ""
    assert not (tmp_path / "report.json").exists()


def test_get_link_propagates_errors_to_the_task_runner_on_airflow_3(airflow_3):
    # Airflow 3's task runner wraps get_link in its own try/except and logs
    # the exception in the task log, so ReportLink must not hide it.
    operator = object()  # no persisted_report_dest attribute

    with pytest.raises(AttributeError):
        ReportLink().get_link(operator, ti_key=MagicMock())
