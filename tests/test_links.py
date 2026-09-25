import logging
from unittest.mock import MagicMock

import pytest

from sparkforensics_operator import links
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.log_ref import LocalEventLog
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


def _xcoms(values):
    return lambda ti_key, key: values.get(key)


def test_get_link_falls_back_to_the_xcom_return_value_on_airflow_2(airflow_2, fake_xcom):
    fake_xcom.get_value.side_effect = _xcoms({"return_value": "s3://reports/app-1.json"})

    assert ReportLink().get_link(MagicMock(), ti_key=MagicMock()) == "s3://reports/app-1.json"


@pytest.mark.parametrize(
    "summary, expected",
    [
        ({"destination": "s3://reports/app-1.json", "report_url": None}, "s3://reports/app-1.json"),
        (
            {"destination": "s3://reports/app-1.json", "report_url": "https://viewer/app-1.json"},
            "https://viewer/app-1.json",
        ),
    ],
    ids=["destination", "report-url"],
)
def test_get_link_reads_the_summary_xcom_on_airflow_2(airflow_2, fake_xcom, summary, expected):
    # The summary is pushed before a breach raises, so it is there even when
    # return_value is not.
    fake_xcom.get_value.side_effect = _xcoms({"sparkforensics_summary": summary})

    assert ReportLink().get_link(MagicMock(), ti_key=MagicMock()) == expected


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
    log_source.locate.return_value = LocalEventLog(tmp_path / "app.log")
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


def test_get_link_returns_the_report_url_when_a_template_is_set_on_airflow_3(airflow_3, tmp_path):
    op = _operator(tmp_path, _report(), report_url_template="https://viewer.example/r?path={path}")
    op.execute({})

    assert ReportLink().get_link(op, ti_key=MagicMock()) == (
        f"https://viewer.example/r?path={tmp_path}/report.json"
    )


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
