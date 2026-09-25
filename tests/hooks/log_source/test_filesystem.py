from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.filesystem import FilesystemLogSourceHook
from sparkforensics_operator.log_ref import LocalEventLog

from ..._render import render


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


def test_locate_returns_the_rendered_path_when_dest_dir_is_not_set(tmp_path):
    log_dir = tmp_path / "logs" / "etl_ar_ventas" / "2026-09-05"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    context = _context(tmp_path)
    hook = render(
        FilesystemLogSourceHook(path_template=str(tmp_path / "logs" / "{{ dag.dag_id }}" / "{{ ds }}" / "app.log")),
        **context,
    )

    result = hook.locate(context).path

    assert result == log_file


def test_locate_copies_into_dest_dir_when_configured(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    dest_dir = tmp_path / "staged"
    hook = FilesystemLogSourceHook(
        path_template=str(log_dir / "app.log"),
        dest_dir=str(dest_dir),
    )

    result = hook.locate(_context(tmp_path)).path

    assert result == dest_dir / "app.log"
    assert result.read_text() == "{}"


def test_locate_raises_when_the_rendered_path_does_not_exist(tmp_path):
    hook = FilesystemLogSourceHook(path_template=str(tmp_path / "missing" / "app.log"))

    with pytest.raises(AirflowException, match="does not exist"):
        hook.locate(_context(tmp_path)).path


def test_cleanup_does_not_touch_the_source_path_when_dest_dir_is_not_set(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"))

    result = hook.locate(_context(tmp_path)).path
    hook.cleanup(LocalEventLog(result))

    assert log_file.exists()


def test_cleanup_removes_the_copy_made_into_dest_dir(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    dest_dir = tmp_path / "staged"
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"), dest_dir=str(dest_dir))

    result = hook.locate(_context(tmp_path)).path
    hook.cleanup(LocalEventLog(result))

    assert not result.exists()
    assert log_file.exists()


@pytest.mark.parametrize(
    "template, context",
    [
        ("{{ run_id }}", {"run_id": "../../../etc/passwd"}),
        ("{{ ti.xcom_pull() }}", {"ti": SimpleNamespace(xcom_pull=lambda: "../../etc")}),
    ],
    ids=["run_id", "xcom-value"],
)
def test_locate_rejects_a_rendered_path_that_climbs_out_of_its_directory(tmp_path, template, context):
    hook = render(FilesystemLogSourceHook(path_template=str(tmp_path / "logs" / template / "app.log")), **context)

    with pytest.raises(AirflowException, match=r"contains a '\.\.' segment"):
        hook.locate(context)


def test_locate_rejects_the_old_str_format_placeholders_with_the_jinja_equivalent(tmp_path):
    hook = FilesystemLogSourceHook(path_template=str(tmp_path / "logs" / "{run_id}" / "app.log"))

    with pytest.raises(AirflowException, match=r"uses the \{run_id\} placeholder.*\{\{ run_id \}\}"):
        hook.locate({"run_id": "manual__1"})


def test_locate_rejects_a_path_that_was_never_rendered(tmp_path):
    hook = FilesystemLogSourceHook(path_template=str(tmp_path / "{{ run_id }}" / "app.log"))

    with pytest.raises(AirflowException, match="was not rendered"):
        hook.locate({"run_id": "manual__1"})


def test_locate_works_when_context_has_no_dag(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "app.log"
    log_file.write_text("{}")
    hook = FilesystemLogSourceHook(path_template=str(log_dir / "app.log"))

    result = hook.locate(_context(tmp_path, dag=None)).path

    assert result == log_file
