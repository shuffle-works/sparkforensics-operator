import json
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG

from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.operator import SparkForensicsOperator, run_spark_forensics
from sparkforensics_operator.report import Report, ThresholdResult


def _report(*threshold_results, exit_code=0):
    return Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[], threshold_results=list(threshold_results),
        exit_code=exit_code,
    )


def _fixtures(tmp_path, report):
    log_source = MagicMock()
    log_source.fetch.return_value = tmp_path / "app.log"
    backend = MagicMock()
    backend.analyze.return_value = report
    return log_source, backend


def test_run_spark_forensics_persists_the_report_and_returns_the_destination(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    dest = tmp_path / "report.json"

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    assert result == str(dest)
    assert dest.exists()


def test_run_spark_forensics_raises_threshold_breached_on_violation_when_failing(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with pytest.raises(ThresholdBreached, match="max-runtime"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )


def test_run_spark_forensics_attaches_the_persisted_destination_to_threshold_breached(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )
    dest = tmp_path / "report.json"

    with pytest.raises(ThresholdBreached) as exc_info:
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(dest),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    # The report was already persisted successfully before this raise; callers
    # that can't get a return value on this path (spark_forensics_callback's
    # _callback) recover the destination from the exception to still push it
    # to XCom (see callback.py).
    assert exc_info.value.destination == str(dest)
    assert dest.exists()


def test_run_spark_forensics_warns_instead_of_raising_when_configured_to_warn(tmp_path, caplog):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with caplog.at_level(logging.WARNING):
        result = run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="warn", notifier=None,
            log=logging.getLogger("test"),
        )

    assert result == str(tmp_path / "report.json")
    assert any("max-runtime" in message for message in caplog.messages)


def test_run_spark_forensics_ignores_violations_when_configured_to_ignore(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={"max_runtime_ms": 1000}, on_threshold_breach="ignore", notifier=None,
        log=logging.getLogger("test"),
    )

    assert result == str(tmp_path / "report.json")


def test_run_spark_forensics_calls_the_notifier_when_configured(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    notifier = MagicMock()

    result = run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=notifier, log=logging.getLogger("test"),
    )

    notifier.notify.assert_called_once_with(backend.analyze.return_value, result)


def test_run_spark_forensics_notifies_even_when_a_breach_raises(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )
    notifier = MagicMock()

    with pytest.raises(ThresholdBreached, match="max-runtime"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=notifier,
            log=logging.getLogger("test"),
        )

    notifier.notify.assert_called_once()


def test_run_spark_forensics_persists_the_full_cli_shape_including_evidence_and_thresholds(tmp_path):
    report = Report(
        schema_version=3, summary={"impactBandCounts": {"critical": 0, "warning": 0, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
        evidence_availability={"taskLevel": False}, detectors=["spill"],
        threshold_results=[ThresholdResult("max-runtime", "pass", "")],
    )
    log_source, backend = _fixtures(tmp_path, report)
    dest = tmp_path / "report.json"

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(dest),
        thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
        log=logging.getLogger("test"),
    )

    persisted = json.loads(dest.read_text())
    assert persisted["evidenceAvailability"] == {"taskLevel": False}
    assert persisted["detectors"] == ["spill"]
    assert persisted["thresholdResults"] == [{"name": "max-runtime", "status": "pass", "detail": ""}]


def test_run_spark_forensics_threads_aws_conn_id_to_sinks_persist(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    with patch("sparkforensics_operator.operator.sinks.persist", return_value="s3://bucket/key") as mock_persist:
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest="s3://bucket/key",
            thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
            aws_conn_id="my_aws_conn",
        )

    assert mock_persist.call_args.kwargs["aws_conn_id"] == "my_aws_conn"


def test_run_spark_forensics_calls_cleanup_on_the_log_source_after_success(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    run_spark_forensics(
        {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
        thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
    )

    log_source.cleanup.assert_called_once_with(log_source.fetch.return_value)


def test_run_spark_forensics_calls_cleanup_even_when_analyze_raises(tmp_path):
    log_source = MagicMock()
    log_source.fetch.return_value = tmp_path / "app.log"
    backend = MagicMock()
    backend.analyze.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="fail", notifier=None, log=logging.getLogger("test"),
        )

    log_source.cleanup.assert_called_once_with(tmp_path / "app.log")


def test_run_spark_forensics_calls_cleanup_even_when_a_breach_raises(tmp_path):
    log_source, backend = _fixtures(
        tmp_path, _report(ThresholdResult("max-runtime", "violation", "too slow")),
    )

    with pytest.raises(ThresholdBreached):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    log_source.cleanup.assert_called_once_with(log_source.fetch.return_value)


def test_run_spark_forensics_treats_exit_code_1_with_no_parsed_violation_as_a_breach(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report(exit_code=1))

    with pytest.raises(ThresholdBreached, match="exit code 1"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )


def test_run_spark_forensics_warns_on_exit_code_3_with_no_parsed_inconclusive_result(tmp_path, caplog):
    log_source, backend = _fixtures(tmp_path, _report(exit_code=3))

    with caplog.at_level(logging.WARNING):
        result = run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={"max_runtime_ms": 1000}, on_threshold_breach="fail", notifier=None,
            log=logging.getLogger("test"),
        )

    # Log-only fallback: no ThresholdBreached, return value unchanged, even
    # with on_threshold_breach="fail" -- exit code 3 is not a violation.
    assert result == str(tmp_path / "report.json")
    assert any("exit code 3" in message for message in caplog.messages)


def test_operator_execute_delegates_to_run_spark_forensics(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    op = SparkForensicsOperator(
        task_id="run_forensics", log_source=log_source, backend=backend,
        report_dest=str(tmp_path / "report.json"),
    )

    result = op.execute({})

    assert result == str(tmp_path / "report.json")


def test_operator_execute_passes_aws_conn_id_to_sinks_persist(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())
    op = SparkForensicsOperator(
        task_id="run_forensics", log_source=log_source, backend=backend,
        report_dest="s3://bucket/key", aws_conn_id="my_aws_conn",
    )

    with patch("sparkforensics_operator.operator.sinks.persist", return_value="s3://bucket/key") as mock_persist:
        op.execute({})

    assert mock_persist.call_args.kwargs["aws_conn_id"] == "my_aws_conn"


def test_operator_rejects_an_invalid_on_threshold_breach_value():
    with pytest.raises(ValueError, match="on_threshold_breach"):
        SparkForensicsOperator(
            task_id="run_forensics", log_source=MagicMock(), backend=MagicMock(),
            report_dest="/tmp/report.json", on_threshold_breach="explode",
        )


def test_run_spark_forensics_rejects_an_invalid_on_threshold_breach_value(tmp_path):
    log_source, backend = _fixtures(tmp_path, _report())

    with pytest.raises(ValueError, match="on_threshold_breach"):
        run_spark_forensics(
            {}, log_source=log_source, backend=backend, report_dest=str(tmp_path / "report.json"),
            thresholds={}, on_threshold_breach="explode", notifier=None,
            log=logging.getLogger("test"),
        )


def test_operator_registers_the_report_link():
    from sparkforensics_operator.links import ReportLink

    assert any(isinstance(link, ReportLink) for link in SparkForensicsOperator.operator_extra_links)


def test_operator_templates_report_dest():
    with DAG(dag_id="sparkforensics_templating", start_date=datetime(2026, 1, 1), schedule=None):
        task = SparkForensicsOperator(
            task_id="run_forensics",
            log_source=MagicMock(),
            backend=MagicMock(),
            report_dest="/tmp/sparkforensics/{{ run_id }}/report.json",
        )

    assert "report_dest" in task.template_fields

    rendered = task.render_template(task.report_dest, {"run_id": "abc123"})

    assert "{{" not in rendered
    assert rendered == "/tmp/sparkforensics/abc123/report.json"
