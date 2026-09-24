from datetime import datetime
from unittest.mock import MagicMock

from airflow import DAG

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS
from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.links import ReportLink
from sparkforensics_operator.operator import SparkForensicsOperator

try:  # Airflow >= 3.1 split DAG serialization out of SerializedDAG
    from airflow.serialization.serialized_objects import DagSerialization as _DagSerializer
except ImportError:
    from airflow.serialization.serialized_objects import SerializedDAG as _DagSerializer


def test_report_link_survives_a_dag_serialization_round_trip():
    # The webserver/API server renders the task page from the serialized
    # DAG, so a link dropped here never shows in the UI. On Airflow 2 this
    # requires ReportLink to be registered via the airflow.plugins entry
    # point (see plugin.py).
    with DAG(dag_id="sparkforensics_serialization", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        SparkForensicsOperator(
            task_id="run_forensics",
            log_source=MagicMock(),
            backend=MagicMock(),
            report_dest="/tmp/sparkforensics/{{ run_id }}/report.json",
        )

    round_tripped = _DagSerializer.from_dict(_DagSerializer.to_dict(dag))
    [link] = round_tripped.task_dict["run_forensics"].operator_extra_links

    assert link.name == ReportLink.name
    if AIRFLOW_V3_PLUS:
        # Airflow 3 rebuilds the link as an XComOperatorLink reading the
        # value the task runner stored under ReportLink's xcom_key.
        assert link.xcom_key == ReportLink().xcom_key
    else:
        assert isinstance(link, ReportLink)


def test_a_deferrable_ssh_operator_survives_a_dag_serialization_round_trip():
    # The scheduler and API server only ever see the serialized DAG; the
    # hooks are rebuilt from the DAG file when the task runs or resumes.
    with DAG(dag_id="sparkforensics_deferrable", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        SparkForensicsOperator(
            task_id="run_forensics",
            log_source=RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{run_id}"),
            backend=SSHAnalyzeHook(ssh_conn_id="onprem_ssh"),
            report_dest="/tmp/sparkforensics/{{ run_id }}/report.json",
            deferrable=True,
        )

    round_tripped = _DagSerializer.from_dict(_DagSerializer.to_dict(dag))

    assert "run_forensics" in round_tripped.task_dict
