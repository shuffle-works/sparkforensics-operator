from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.log_ref import RemoteEventLog

from ..._render import render


def test_locate_renders_the_path_template_without_touching_the_remote_host():
    task = MagicMock()
    task.task_id = "spark_job"
    context = {"run_id": "manual__2026-01-01", "task": task}
    hook = render(
        RemotePathLogSourceHook(ssh_conn_id="{{ 'onprem' }}_ssh", path_template="/logs/{{ task.task_id }}/{{ run_id }}"),
        **context,
    )

    log_ref = hook.locate(context)

    assert log_ref == RemoteEventLog(ssh_conn_id="onprem_ssh", path="/logs/spark_job/manual__2026-01-01")


def test_locate_rejects_a_path_traversal_run_id():
    context = {"run_id": "../../../etc/passwd"}
    hook = render(RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{{ run_id }}"), **context)

    with pytest.raises(AirflowException, match=r"contains a '\.\.' segment"):
        hook.locate(context)


def test_locate_rejects_a_template_that_renders_to_an_empty_path():
    context = {"run_id": ""}
    hook = render(RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="{{ run_id }}"), **context)

    with pytest.raises(AirflowException, match="empty path"):
        hook.locate(context)
