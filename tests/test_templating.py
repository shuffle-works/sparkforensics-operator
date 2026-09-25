"""Hook arguments are Jinja templates Airflow renders as nested template
fields of SparkForensicsOperator; the callback form renders them itself."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator import (
    FilesystemLogSourceHook,
    HistoryServerAppLogSourceHook,
    HistoryServerLogSourceHook,
    RemotePathLogSourceHook,
    SFTPLogSourceHook,
    SSHAnalyzeHook,
    SSHTunneledLogSourceHook,
    SubprocessAnalyzeHook,
    XComLogSourceHook,
    spark_forensics_callback,
)
from sparkforensics_operator.log_ref import HistoryServerApp, RemoteEventLog
from sparkforensics_operator.operator import SparkForensicsOperator
from sparkforensics_operator.report import Report

from ._render import render

CONTEXT = {"run_id": "manual__2026-01-01", "ds": "2026-01-01", "params": {"app": "app-42", "attempt": "2"}}


@pytest.mark.parametrize(
    "hook",
    [
        FilesystemLogSourceHook(path_template="/logs/{{ run_id }}", dest_dir="/stage/{{ ds }}"),
        SFTPLogSourceHook(ssh_conn_id="{{ params.app }}", path_template="/logs/{{ run_id }}", dest_dir="/stage/{{ ds }}"),
        RemotePathLogSourceHook(ssh_conn_id="{{ params.app }}", path_template="/logs/{{ run_id }}"),
        HistoryServerLogSourceHook(base_url="http://{{ ds }}", app_id="{{ params.app }}", attempt_id="{{ params.attempt }}", dest_dir="/stage/{{ ds }}"),
        HistoryServerAppLogSourceHook(base_url="http://{{ ds }}", app_id="{{ params.app }}", attempt_id="{{ params.attempt }}"),
        XComLogSourceHook(task_id="{{ params.app }}", xcom_key="{{ ds }}"),
        SSHTunneledLogSourceHook(ssh_conn_id="{{ params.app }}", remote_host="{{ ds }}", remote_port=18080, hook_factory=MagicMock()),
        SubprocessAnalyzeHook(analyze_bin="/opt/{{ ds }}/sparkforensics-analyze"),
        SSHAnalyzeHook(ssh_conn_id="{{ params.app }}", analyze_bin="/opt/{{ ds }}/bin", remote_base_dir="/jobs/{{ ds }}"),
    ],
    ids=lambda hook: type(hook).__name__,
)
def test_every_template_field_of_every_hook_is_rendered_by_the_operator(hook):
    rendered = render(hook, **CONTEXT)

    for name in type(hook).template_fields:
        value = getattr(rendered, name)
        assert "{{" not in value, f"{name} was not rendered: {value!r}"
    assert type(hook).template_fields, "every hook declares what it templates"


def test_a_history_server_app_id_can_come_from_an_upstream_xcom():
    ti = SimpleNamespace(xcom_pull=lambda task_ids, key: {"app_id": "app-7", "attempt": None}[key])
    context = {"ti": ti}
    hook = render(
        HistoryServerAppLogSourceHook(
            base_url="http://localhost:18080/",
            app_id="{{ ti.xcom_pull(task_ids='spark', key='app_id') }}",
            attempt_id="{{ ti.xcom_pull(task_ids='spark', key='attempt') }}",
        ),
        **context,
    )

    # A missing XCom renders as "None"; it must not reach the CLI as an id.
    assert hook.locate(context) == HistoryServerApp(base_url="http://localhost:18080", app_id="app-7")


@pytest.mark.parametrize("app_id", ["", "None"])
def test_an_app_id_that_rendered_to_nothing_fails_clearly(app_id):
    hook = HistoryServerAppLogSourceHook(base_url="http://localhost:18080", app_id=app_id)

    with pytest.raises(AirflowException, match="app_id rendered to"):
        hook.locate({})


def test_a_templated_remote_base_dir_is_checked_once_rendered():
    hook = render(SSHAnalyzeHook(ssh_conn_id="onprem_ssh", remote_base_dir="{{ params.dir }}"), params={"dir": "relative"})

    with pytest.raises(ValueError, match="remote_base_dir must be an absolute path"):
        hook.abandon({"ti": SimpleNamespace(dag_id="d", task_id="t", run_id="r", map_index=-1)})


def test_the_tunnel_renders_the_hook_its_factory_builds():
    pytest.importorskip("airflow.providers.ssh.hooks.ssh")
    context = {"run_id": "manual__1", "params": {"app": "app-9"}}
    inner = {}

    def factory(base_url):
        inner["hook"] = HistoryServerLogSourceHook(base_url=base_url, app_id="{{ params.app }}")
        inner["hook"].locate = MagicMock(return_value=MagicMock(spec=["path"]))
        return inner["hook"]

    op = SparkForensicsOperator(
        task_id="t",
        log_source=SSHTunneledLogSourceHook(ssh_conn_id="onprem_ssh", remote_host="shs", remote_port=18080, hook_factory=factory),
        backend=SubprocessAnalyzeHook(),
        report_dest="/tmp/r.json",
    )
    op.render_template_fields(context)
    tunnel = MagicMock()
    tunnel.__enter__.return_value.local_bind_port = 5555
    ssh_hook = MagicMock()
    ssh_hook.get_tunnel.return_value = tunnel

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook):
        with pytest.raises(AirflowException):  # the mocked inner result is no LocalEventLog
            op.log_source.locate({**context, "task": op})

    assert inner["hook"].app_id == "app-9"


def test_tasks_sharing_one_hook_instance_each_render_their_own_copy():
    shared = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{{ run_id }}")
    first, second = (
        SparkForensicsOperator(task_id=f"t{i}", log_source=shared, backend=SubprocessAnalyzeHook(), report_dest="/tmp/r.json")
        for i in (1, 2)
    )

    first.render_template_fields({"run_id": "run_1"})
    second.render_template_fields({"run_id": "run_2"})

    assert first.log_source.locate({}).path == "/logs/run_1"
    assert second.log_source.locate({}).path == "/logs/run_2"
    assert shared.path_template == "/logs/{{ run_id }}"


def test_a_hooks_repr_shows_its_rendered_template_fields():
    hook = render(RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{{ run_id }}"), run_id="r1")

    assert repr(hook) == "RemotePathLogSourceHook(ssh_conn_id='onprem_ssh', path_template='/logs/r1')"


def _report():
    return Report(
        schema_version=3, summary={"impactBandCounts": {}}, findings=[], recommendations=[], clean_checks=[],
    )


def test_the_callback_renders_report_dest_and_hook_arguments_with_the_upstream_tasks_context(tmp_path):
    log_source = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{{ run_id }}")
    backend = MagicMock(spec=["analyze"])
    backend.analyze.return_value = _report()
    callback = spark_forensics_callback(
        log_source=log_source, backend=backend, report_dest=str(tmp_path / "{{ run_id }}" / "report.json"),
    )
    upstream = SparkForensicsOperator(task_id="spark", log_source=log_source, backend=backend, report_dest="x")

    for run_id in ("run_1", "run_2"):
        callback({"run_id": run_id, "task": upstream, "ti": MagicMock()})
        assert backend.analyze.call_args.args[0] == RemoteEventLog("onprem_ssh", f"/logs/{run_id}")
        assert (tmp_path / run_id / "report.json").exists()

    # The hook passed to the factory is shared by every run: never rendered in place.
    assert log_source.path_template == "/logs/{{ run_id }}"


class _JsonTemplatingOperator(SparkForensicsOperator):
    # Like EmrAddStepsOperator: its own template fields may name .json template files.
    template_ext = (".json",)


def test_the_callback_never_reads_its_values_as_the_upstream_tasks_template_files(tmp_path):
    log_source = RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/logs/{{ run_id }}/app.json")
    backend = MagicMock(spec=["analyze"])
    backend.analyze.return_value = _report()
    callback = spark_forensics_callback(
        log_source=log_source, backend=backend, report_dest=str(tmp_path / "{{ run_id }}" / "app.json"),
    )
    upstream = _JsonTemplatingOperator(task_id="spark", log_source=log_source, backend=backend, report_dest="x")

    callback({"run_id": "run_1", "task": upstream, "ti": MagicMock()})

    assert backend.analyze.call_args.args[0] == RemoteEventLog("onprem_ssh", "/logs/run_1/app.json")
    assert (tmp_path / "run_1" / "app.json").exists()
    assert upstream.template_ext == (".json",)


def test_the_tunnel_never_reads_its_inner_hooks_values_as_the_tasks_template_files():
    pytest.importorskip("airflow.providers.ssh.hooks.ssh")
    inner = {}

    def factory(base_url):
        inner["hook"] = HistoryServerLogSourceHook(base_url=base_url, app_id="{{ params.app }}.json")
        inner["hook"].locate = MagicMock(return_value=MagicMock(spec=["path"]))
        return inner["hook"]

    hook = SSHTunneledLogSourceHook(ssh_conn_id="onprem_ssh", remote_host="shs", remote_port=18080, hook_factory=factory)
    upstream = _JsonTemplatingOperator(task_id="spark", log_source=hook, backend=SubprocessAnalyzeHook(), report_dest="x")
    tunnel = MagicMock()
    tunnel.__enter__.return_value.local_bind_port = 5555
    ssh_hook = MagicMock()
    ssh_hook.get_tunnel.return_value = tunnel

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=ssh_hook):
        with pytest.raises(AirflowException):  # the mocked inner result is no LocalEventLog
            hook.locate({"params": {"app": "app-9"}, "task": upstream})

    assert inner["hook"].app_id == "app-9.json"
