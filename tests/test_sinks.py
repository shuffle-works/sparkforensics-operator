import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sparkforensics_operator import sinks

SAMPLE_REPORT = {"schemaVersion": 3, "findings": []}


def test_persist_writes_to_a_plain_local_path(tmp_path):
    dest = tmp_path / "reports" / "app-1.json"

    result = sinks.persist(SAMPLE_REPORT, str(dest))

    assert result == str(dest)
    assert json.loads(dest.read_text()) == SAMPLE_REPORT


def test_persist_writes_to_a_file_uri(tmp_path):
    dest_path = tmp_path / "app-1.json"

    result = sinks.persist(SAMPLE_REPORT, f"file://{dest_path}")

    assert result == str(dest_path)
    assert json.loads(dest_path.read_text()) == SAMPLE_REPORT


def test_persist_writes_atomically_via_a_temp_file_and_replace(tmp_path):
    dest = tmp_path / "reports" / "app-1.json"

    with patch("sparkforensics_operator.sinks.os.replace", wraps=sinks.os.replace) as mock_replace:
        result = sinks.persist(SAMPLE_REPORT, str(dest))

    assert result == str(dest)
    mock_replace.assert_called_once()
    tmp_arg, dest_arg = mock_replace.call_args[0]
    assert Path(tmp_arg).parent == dest.parent
    assert Path(tmp_arg) != dest
    assert Path(dest_arg) == dest
    assert json.loads(dest.read_text()) == SAMPLE_REPORT


def test_persist_uses_umask_derived_permissions_not_the_tempfile_default(tmp_path):
    dest = tmp_path / "reports" / "app-1.json"

    previous_umask = os.umask(0o022)
    try:
        sinks.persist(SAMPLE_REPORT, str(dest))
    finally:
        os.umask(previous_umask)

    mode = os.stat(dest).st_mode & 0o777
    assert mode == 0o644
    assert mode != 0o600  # tempfile.NamedTemporaryFile's hardcoded default


def test_persist_uploads_to_s3():
    mock_hook = MagicMock()
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook):
        result = sinks.persist(SAMPLE_REPORT, "s3://my-bucket/reports/app-1.json")

    assert result == "s3://my-bucket/reports/app-1.json"
    mock_hook.load_string.assert_called_once()
    _, kwargs = mock_hook.load_string.call_args
    assert kwargs["bucket_name"] == "my-bucket"
    assert kwargs["key"] == "reports/app-1.json"
    assert kwargs["replace"] is True


def test_persist_rejects_an_unsupported_scheme():
    with pytest.raises(ValueError, match="Unsupported report_dest scheme"):
        sinks.persist(SAMPLE_REPORT, "ftp://example.com/app-1.json")


def test_persist_rejects_a_file_uri_with_a_host():
    with pytest.raises(ValueError, match="file:// URIs with a host are not supported"):
        sinks.persist(SAMPLE_REPORT, "file://reports/app-1.json")


def test_persist_uses_a_custom_aws_conn_id():
    mock_hook = MagicMock()
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook) as mock_cls:
        sinks.persist(
            SAMPLE_REPORT, "s3://my-bucket/reports/app-1.json", aws_conn_id="my_aws_conn",
        )

    mock_cls.assert_called_once_with(aws_conn_id="my_aws_conn")
    mock_hook.load_string.assert_called_once()


def test_persist_uses_the_default_aws_connection_when_none_is_given():
    mock_hook = MagicMock()
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook) as mock_cls:
        sinks.persist(SAMPLE_REPORT, "s3://my-bucket/reports/app-1.json")

    mock_cls.assert_called_once_with()


def test_persist_succeeds_when_no_aws_connection_is_defined():
    # AirflowNotFoundException is what S3Hook(...).get_connection actually
    # raises when no Connection row exists for aws_conn_id_used -- a
    # legitimate production setup where AWS auth is handled entirely outside
    # Airflow (IAM instance role, env vars, shared credentials profile).
    # AwsGenericHook.conn_config catches this exact exception internally and
    # falls back gracefully, so persist() must not turn it into a hard
    # failure either.
    from airflow.exceptions import AirflowNotFoundException

    mock_hook = MagicMock()
    mock_hook.get_connection.side_effect = AirflowNotFoundException("The conn_id `aws_default` isn't defined")
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook):
        result = sinks.persist(SAMPLE_REPORT, "s3://my-bucket/reports/app-1.json")

    assert result == "s3://my-bucket/reports/app-1.json"
    mock_hook.get_connection.assert_called_once_with("aws_default")
    mock_hook.load_string.assert_called_once()


def test_persist_wraps_a_broken_aws_connection_lookup_in_an_airflow_exception():
    from airflow.exceptions import AirflowException

    # S3Hook's __init__ never performs the connection lookup itself (that's a
    # lazy cached_property triggered on first client use); mock get_connection
    # -- the method persist() now calls eagerly -- raising instead, so this
    # exercises the real lazy-lookup failure mode rather than a construction
    # failure that can't actually happen.
    mock_hook = MagicMock()
    mock_hook.get_connection.side_effect = Exception("no such connection")
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook):
        with pytest.raises(AirflowException, match="aws_conn_id") as exc_info:
            sinks.persist(SAMPLE_REPORT, "s3://my-bucket/reports/app-1.json")

    mock_hook.get_connection.assert_called_once_with("aws_default")
    mock_hook.load_string.assert_not_called()
    assert exc_info.value.__cause__ is not None


def test_persist_wraps_a_broken_aws_connection_lookup_with_explicit_conn_id():
    from airflow.exceptions import AirflowException

    mock_hook = MagicMock()
    mock_hook.get_connection.side_effect = Exception("connection misconfigured")
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock_hook):
        with pytest.raises(AirflowException, match="aws_conn_id") as exc_info:
            sinks.persist(
                SAMPLE_REPORT,
                "s3://my-bucket/reports/app-1.json",
                aws_conn_id="my_aws_conn",
            )

    mock_hook.get_connection.assert_called_once_with("my_aws_conn")
    mock_hook.load_string.assert_not_called()
    assert exc_info.value.__cause__ is not None
