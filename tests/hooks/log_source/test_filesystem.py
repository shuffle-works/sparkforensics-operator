from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.filesystem import FilesystemLogSourceHook


def _context(tmp_path, **overrides):
    dag = MagicMock()
    dag.dag_id = "etl_ar_ventas"
    task = MagicMock()
    task.task_id = "run_spark_job"
    base = {
        "ds": "2026-09-05",
        "run_id": "scheduled__2026-09-05T00:00:00",
        "logical_date": "2026-09-05T00:00:00",
        "dag": dag,
        "task": task,
    }
    base.update(overrides)
    return base


def test_fetch_returns_the_rendered_path_when_dest_dir_is_not_set(tmp_path):
    log_dir = tmp_path / "logs" / "etl_ar_ventas" / "2026-09-05"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(tmp_path / "logs" / "{dag_id}" / "{ds}" / "app.log"))

    result = hook.fetch(_context(tmp_path))

    assert result == log_file


def test_fetch_copies_into_dest_dir_when_configured(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    dest_dir = tmp_path / "staged"
    hook = FilesystemLogSourceHook(
        path_template=str(log_dir / "app.log"),
        dest_dir=str(dest_dir),
    )

    result = hook.fetch(_context(tmp_path))

    assert result == dest_dir / "app.log"
    assert result.read_text() == "{}"


def test_fetch_raises_when_the_rendered_path_does_not_exist(tmp_path):
    hook = FilesystemLogSourceHook(path_template=str(tmp_path / "missing" / "app.log"))

    with pytest.raises(AirflowException, match="does not exist"):
        hook.fetch(_context(tmp_path))


def test_cleanup_does_not_touch_the_source_path_when_dest_dir_is_not_set(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"))

    result = hook.fetch(_context(tmp_path))
    hook.cleanup(result)

    assert log_file.exists()


def test_cleanup_removes_the_copy_made_into_dest_dir(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    dest_dir = tmp_path / "staged"
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"), dest_dir=str(dest_dir))

    result = hook.fetch(_context(tmp_path))
    hook.cleanup(result)

    assert not result.exists()
    assert log_file.exists()


def test_fetch_sanitizes_a_path_traversal_run_id(tmp_path):
    base_dir = tmp_path / "logs"
    log_dir = base_dir / "passwd"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(base_dir / "{run_id}" / "app.log"))

    result = hook.fetch(_context(tmp_path, run_id="../../../etc/passwd"))

    assert result == base_dir / "passwd" / "app.log"
    assert base_dir in result.parents


def test_fetch_sanitizes_a_path_traversal_dag_id(tmp_path):
    dag = MagicMock()
    dag.dag_id = "../../../etc/passwd"
    base_dir = tmp_path / "logs"
    log_dir = base_dir / "passwd"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(base_dir / "{dag_id}" / "app.log"))

    result = hook.fetch(_context(tmp_path, dag=dag))

    assert result == base_dir / "passwd" / "app.log"
    assert base_dir in result.parents


def test_fetch_sanitizes_a_path_traversal_task_id(tmp_path):
    task = MagicMock()
    task.task_id = "../../../etc/passwd"
    base_dir = tmp_path / "logs"
    log_dir = base_dir / "passwd"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(base_dir / "{task_id}" / "app.log"))

    result = hook.fetch(_context(tmp_path, task=task))

    assert result == base_dir / "passwd" / "app.log"
    assert base_dir in result.parents


def test_fetch_works_when_context_has_no_dag(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"))

    result = hook.fetch(_context(tmp_path, dag=None))

    assert result == log_file
