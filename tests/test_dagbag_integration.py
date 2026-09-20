from datetime import datetime
from unittest.mock import MagicMock

from airflow import DAG

from sparkforensics_operator.operator import SparkForensicsOperator


def test_operator_wires_into_a_minimal_dag_without_import_errors():
    with DAG(dag_id="sparkforensics_smoke", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        task = SparkForensicsOperator(
            task_id="run_forensics",
            log_source=MagicMock(),
            backend=MagicMock(),
            report_dest="/tmp/sparkforensics/{{ run_id }}/report.json",
            max_runtime_ms=3_600_000,
            on_threshold_breach="warn",
        )

    assert dag.task_dict["run_forensics"] is task
    assert task.on_threshold_breach == "warn"
    assert task.thresholds["max_runtime_ms"] == 3_600_000
