from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.sftp import SFTPLogSourceHook
from sparkforensics_operator.log_ref import LocalEventLog

pytest.importorskip("airflow.providers.sftp.hooks.sftp")


def _mock_sftp_hook(path_exists=True, isdir=False, list_directory=None):
    hook = MagicMock()
    hook.path_exists.return_value = path_exists
    hook.isdir.return_value = isdir
    hook.list_directory.return_value = list_directory or []
    return hook


def _writing_retrieve_file(content: bytes = b"{}"):
    def _retrieve(remote_full_path, local_full_path, prefetch=True):
        Path(local_full_path).write_bytes(content)
    return _retrieve


def test_resolve_retrieves_a_single_remote_file_into_dest_dir(tmp_path):
    dest_dir = tmp_path / "staged"
    mock_hook = _mock_sftp_hook()
    mock_hook.retrieve_file.side_effect = _writing_retrieve_file()
    hook = SFTPLogSourceHook(
        ssh_conn_id="onprem_ssh", path_template="/onprem/logs/app.log", dest_dir=str(dest_dir),
    )

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook) as mock_cls:
        result = hook.resolve({}).path

    mock_cls.assert_called_once_with(ssh_conn_id="onprem_ssh")
    mock_hook.path_exists.assert_called_once_with("/onprem/logs/app.log")
    mock_hook.retrieve_file.assert_called_once_with(
        "/onprem/logs/app.log", str(dest_dir / "app.log"),
    )
    assert result == dest_dir / "app.log"
    assert result.read_text() == "{}"


def test_resolve_renders_the_path_template_against_context(tmp_path):
    dest_dir = tmp_path / "staged"
    mock_hook = _mock_sftp_hook()
    mock_hook.retrieve_file.side_effect = _writing_retrieve_file()
    hook = SFTPLogSourceHook(
        ssh_conn_id="onprem_ssh",
        path_template="/onprem/logs/{dag_id}/{ds}/app.log",
        dest_dir=str(dest_dir),
    )
    dag = MagicMock()
    dag.dag_id = "etl_ar_ventas"
    context = {"ds": "2026-09-05", "dag": dag, "task": None}

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        hook.resolve(context).path

    mock_hook.path_exists.assert_called_once_with("/onprem/logs/etl_ar_ventas/2026-09-05/app.log")


def test_resolve_returns_a_directory_for_a_rolling_log(tmp_path):
    dest_dir = tmp_path / "staged"
    mock_hook = _mock_sftp_hook(
        isdir=True,
        list_directory=["events_1_app", "events_2_app", "unrelated_file"],
    )
    mock_hook.retrieve_file.side_effect = _writing_retrieve_file()
    hook = SFTPLogSourceHook(
        ssh_conn_id="onprem_ssh", path_template="/onprem/logs/rolling", dest_dir=str(dest_dir),
    )

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        result = hook.resolve({}).path

    source_dir = dest_dir / "rolling"
    assert result == source_dir
    assert mock_hook.retrieve_file.call_count == 2
    mock_hook.retrieve_file.assert_any_call(
        "/onprem/logs/rolling/events_1_app", str(source_dir / "events_1_app"),
    )
    mock_hook.retrieve_file.assert_any_call(
        "/onprem/logs/rolling/events_2_app", str(source_dir / "events_2_app"),
    )


def test_resolve_raises_when_a_rolling_log_directory_has_no_matching_entries(tmp_path):
    dest_dir = tmp_path / "staged"
    mock_hook = _mock_sftp_hook(isdir=True, list_directory=["readme.txt", "other"])
    hook = SFTPLogSourceHook(
        ssh_conn_id="onprem_ssh", path_template="/onprem/logs/rolling", dest_dir=str(dest_dir),
    )

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        with pytest.raises(AirflowException, match="Unexpected SFTP log directory contents"):
            hook.resolve({}).path


def test_resolve_cleans_up_its_own_temp_dir_when_a_rolling_log_directory_has_no_matching_entries():
    mock_hook = _mock_sftp_hook(isdir=True, list_directory=["readme.txt"])
    hook = SFTPLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/onprem/logs/rolling")

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        with pytest.raises(AirflowException):
            hook.resolve({}).path

    assert hook._owned_temp_root is not None
    assert not hook._owned_temp_root.exists()


def test_resolve_raises_when_the_remote_path_does_not_exist(tmp_path):
    mock_hook = _mock_sftp_hook(path_exists=False)
    hook = SFTPLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/onprem/missing")

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        with pytest.raises(AirflowException, match="does not exist"):
            hook.resolve({}).path


def test_resolve_cleans_up_its_own_temp_dir_when_the_remote_path_is_missing():
    mock_hook = _mock_sftp_hook(path_exists=False)
    hook = SFTPLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/onprem/missing")

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        with pytest.raises(AirflowException):
            hook.resolve({}).path

    assert hook._owned_temp_root is not None
    assert not hook._owned_temp_root.exists()


def test_resolve_wraps_a_transport_failure_in_an_airflowexception():
    hook = SFTPLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/onprem/logs/app.log")

    with patch(
        "airflow.providers.sftp.hooks.sftp.SFTPHook", side_effect=OSError("no route to host"),
    ):
        with pytest.raises(AirflowException, match="onprem_ssh"):
            hook.resolve({}).path


def test_cleanup_removes_the_private_temp_dir_when_dest_dir_is_not_set():
    mock_hook = _mock_sftp_hook()
    mock_hook.retrieve_file.side_effect = _writing_retrieve_file()
    hook = SFTPLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/onprem/logs/app.log")

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        result = hook.resolve({}).path

    owned_root = hook._owned_temp_root
    assert owned_root is not None and owned_root.exists()

    hook.cleanup(LocalEventLog(result))

    assert not owned_root.exists()


def test_cleanup_does_not_touch_a_caller_provided_dest_dir(tmp_path):
    dest_dir = tmp_path / "staged"
    mock_hook = _mock_sftp_hook()
    mock_hook.retrieve_file.side_effect = _writing_retrieve_file()
    hook = SFTPLogSourceHook(
        ssh_conn_id="onprem_ssh", path_template="/onprem/logs/app.log", dest_dir=str(dest_dir),
    )

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHook", return_value=mock_hook):
        result = hook.resolve({}).path

    hook.cleanup(LocalEventLog(result))

    assert dest_dir.exists()
