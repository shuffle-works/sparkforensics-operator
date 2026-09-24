import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.history_server import HistoryServerLogSourceHook


def _zip_bytes(entries: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _fake_response(body: bytes, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.iter_content.return_value = [body]
    return response


def test_fetch_builds_the_shs_logs_url_and_extracts_a_single_entry(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="application_123_0001",
        dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"application_123_0001": "{}"})

    with patch("requests.get", return_value=_fake_response(body)) as mock_get:
        result = hook.fetch({})

    mock_get.assert_called_once_with(
        "http://shs.internal:18080/api/v1/applications/application_123_0001/logs",
        timeout=300, stream=True,
    )
    assert result.name == "application_123_0001"
    assert result.read_text() == "{}"


def test_fetch_includes_attempt_id_in_the_url_when_configured(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="application_123_0001",
        attempt_id="1",
        dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"application_123_0001_1": "{}"})

    with patch("requests.get", return_value=_fake_response(body)) as mock_get:
        hook.fetch({})

    mock_get.assert_called_once_with(
        "http://shs.internal:18080/api/v1/applications/application_123_0001/1/logs",
        timeout=300, stream=True,
    )


def _rolling_zip_bytes(app_id: str, with_dir_entry: bool = True) -> bytes:
    # Mirrors Spark's RollingEventLogFilesFileReader.zipEventLogFiles: an
    # "eventlog_v2_<appId>/" directory entry, then every file in the rolling
    # folder (the appstatus marker and the events_<n>_ segments) under it.
    folder = f"eventlog_v2_{app_id}/"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        if with_dir_entry:
            zf.writestr(folder, "")
        zf.writestr(f"{folder}appstatus_{app_id}", "")
        zf.writestr(f"{folder}events_1_{app_id}", "{}")
        zf.writestr(f"{folder}events_2_{app_id}", "{}")
    return buf.getvalue()


@pytest.mark.parametrize("with_dir_entry", [True, False])
def test_fetch_returns_the_rolling_folder_for_a_rolling_log(tmp_path, with_dir_entry):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="application_123_0001",
        dest_dir=str(tmp_path),
    )
    body = _rolling_zip_bytes("application_123_0001", with_dir_entry=with_dir_entry)

    with patch("requests.get", return_value=_fake_response(body)):
        result = hook.fetch({})

    # sparkforensics-analyze only inspects the direct children of the path it
    # receives, so the returned directory must hold the events_* files itself.
    assert result.is_dir()
    assert result.name == "eventlog_v2_application_123_0001"
    assert sorted(p.name for p in result.iterdir()) == [
        "appstatus_application_123_0001",
        "events_1_application_123_0001",
        "events_2_application_123_0001",
    ]


@pytest.mark.parametrize(
    "entries",
    [
        # Rolling segments not under any folder.
        {"events_1_application_123_0001": "{}", "events_2_application_123_0001": "{}"},
        # Rolling segments split across two folders (e.g. two attempts).
        {
            "eventlog_v2_application_123_0001_1/events_1_application_123_0001_1": "{}",
            "eventlog_v2_application_123_0001_2/events_1_application_123_0001_2": "{}",
        },
        # A rolling folder plus a stray top-level entry.
        {
            "eventlog_v2_application_123_0001/events_1_application_123_0001": "{}",
            "stray_entry": "{}",
        },
        # Rolling segments nested one level too deep.
        {"outer/eventlog_v2_application_123_0001/events_1_application_123_0001": "{}"},
    ],
)
def test_fetch_raises_when_entries_are_not_under_exactly_one_folder(tmp_path, entries):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001", dest_dir=str(tmp_path),
    )

    with patch("requests.get", return_value=_fake_response(_zip_bytes(entries))):
        with pytest.raises(AirflowException, match="not under exactly one folder"):
            hook.fetch({})


def test_fetch_raises_when_the_single_folder_has_no_rolling_segments(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001", dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"some_folder/unexpected_entry": "{}", "some_folder/another": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        with pytest.raises(AirflowException, match="no events_<n>_"):
            hook.fetch({})


def test_fetch_raises_on_a_non_200_response(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001", dest_dir=str(tmp_path),
    )

    with patch("requests.get", return_value=_fake_response(b"", status_code=404)):
        with pytest.raises(AirflowException, match="404"):
            hook.fetch({})


def test_fetch_sanitizes_a_path_traversal_app_id(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="../../etc/application_123_0001",
        dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"application_123_0001": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        result = hook.fetch({})

    assert tmp_path in result.parents
    assert result.name == "application_123_0001"


def test_cleanup_removes_the_hooks_own_temp_dir_when_dest_dir_is_not_set(tmp_path):
    hook = HistoryServerLogSourceHook(base_url="http://shs.internal:18080", app_id="application_123_0001")
    body = _zip_bytes({"application_123_0001": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        result = hook.fetch({})

    owned_root = hook._owned_temp_root
    assert owned_root is not None and owned_root.exists()

    hook.cleanup(result)

    assert not owned_root.exists()


def test_cleanup_does_not_touch_a_caller_provided_dest_dir(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001", dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"application_123_0001": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        result = hook.fetch({})

    hook.cleanup(result)

    assert tmp_path.exists()


def test_fetch_cleans_up_owned_temp_dir_on_invalid_zip_failure():
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001"
    )
    # A response that returns 200 but whose body is not a valid zip
    with patch("requests.get", return_value=_fake_response(b"not a zip")):
        with pytest.raises(Exception):  # zipfile.BadZipFile or similar
            hook.fetch({})

    owned_root = hook._owned_temp_root
    assert owned_root is not None
    assert not owned_root.exists()


def test_fetch_cleans_up_owned_temp_dir_on_unexpected_archive_layout():
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001"
    )
    # A zip with neither a single entry nor events_<n>_ rolling entries
    body = _zip_bytes({"unexpected_entry": "{}", "another_unexpected": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        with pytest.raises(AirflowException, match="Unexpected Spark History Server log archive"):
            hook.fetch({})

    owned_root = hook._owned_temp_root
    assert owned_root is not None
    assert not owned_root.exists()


def test_fetch_raises_if_the_total_download_exceeds_the_timeout(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="application_123_0001",
        dest_dir=str(tmp_path),
        timeout=300,
    )
    body = _zip_bytes({"application_123_0001": "{}"})
    # Split the body into several chunks so the download loop iterates
    # multiple times, each one calling response.iter_content once per chunk.
    chunks = [body[:1], body[1:2], body[2:]]
    response = _fake_response(body, status_code=200)
    response.iter_content.return_value = chunks

    # time.monotonic() is called once to set the deadline (start + timeout),
    # then once per chunk written. Keep the first two chunk checks under the
    # deadline, then cross it on the third so the loop trips mid-download
    # rather than trivially on the first check.
    start = 1_000.0
    monotonic_values = [start, start + 1, start + 2, start + 301]

    with patch("requests.get", return_value=response):
        with patch(
            "sparkforensics_operator.hooks.log_source.history_server.time.monotonic",
            side_effect=monotonic_values,
        ):
            with pytest.raises(AirflowException, match="exceeded"):
                hook.fetch({})

    response.close.assert_called_once()


def test_fetch_with_dest_dir_does_not_delete_caller_provided_dir_on_failure(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080",
        app_id="application_123_0001",
        dest_dir=str(tmp_path),
    )
    # A response that returns 200 but whose body is not a valid zip
    with patch("requests.get", return_value=_fake_response(b"not a zip")):
        with pytest.raises(Exception):  # zipfile.BadZipFile or similar
            hook.fetch({})

    assert tmp_path.exists()
    assert hook._owned_temp_root is None


def test_fetch_rejects_an_absolute_rolling_entry_path_instead_of_returning_the_filesystem_root(tmp_path):
    hook = HistoryServerLogSourceHook(
        base_url="http://shs.internal:18080", app_id="application_123_0001", dest_dir=str(tmp_path),
    )
    body = _zip_bytes({"/events_1_application_123_0001": "{}", "/events_2_application_123_0001": "{}"})

    with patch("requests.get", return_value=_fake_response(body)):
        with pytest.raises(AirflowException, match="Unexpected Spark History Server log archive"):
            hook.fetch({})
