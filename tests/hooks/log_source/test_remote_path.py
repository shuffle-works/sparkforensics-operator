from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.log_ref import RemoteEventLog


def test_resolve_renders_the_path_template_without_touching_the_remote_host():
    task = MagicMock()
    task.task_id = "spark_job"
    hook = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{task_id}/{run_id}")

    log_ref = hook.resolve({"run_id": "manual__2026-01-01", "task": task})

    assert log_ref == RemoteEventLog(ssh_conn_id="onprem_ssh", path="/logs/spark_job/manual__2026-01-01")


def test_resolve_sanitizes_a_path_traversal_run_id():
    hook = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{run_id}")

    log_ref = hook.resolve({"run_id": "../../../etc/passwd"})

    assert log_ref.path == "/logs/passwd"


def test_resolve_rejects_a_template_that_renders_to_an_empty_path():
    hook = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="{run_id}")

    with pytest.raises(AirflowException, match="empty remote path"):
        hook.resolve({"run_id": ""})
