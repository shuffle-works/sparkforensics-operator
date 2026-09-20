from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from airflow.exceptions import AirflowException, AirflowNotFoundException


def persist(report_json: dict, report_dest: str, aws_conn_id: str | None = None) -> str:
    """Writes the report JSON to report_dest and returns the destination
    string (used for the XCom push and ReportLink). report_dest is a plain
    local path, a file:// URI, or an s3://bucket/key URI. An HDFS
    destination is just a local path under an already-mounted filesystem
    (NFS gateway / WebHDFS FUSE mount), same approach as
    FilesystemLogSourceHook. aws_conn_id targets a non-default Airflow AWS
    connection for the s3:// case; ignored otherwise."""
    parsed = urlparse(report_dest)
    payload = json.dumps(report_json, indent=2)

    if parsed.scheme in ("", "file"):
        if parsed.scheme == "file":
            if parsed.netloc not in ("", "localhost"):
                raise ValueError(
                    f"file:// URIs with a host are not supported: {report_dest!r} "
                    "(use file:///absolute/path or a plain local path)."
                )
            dest_path = Path(parsed.path)
        else:
            dest_path = Path(report_dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        # Write to a temp file in the same directory as dest_path, then
        # os.replace it into place: this is atomic (same filesystem), so
        # concurrent writers can never observe a partially-written file.
        tmp_file = tempfile.NamedTemporaryFile(
            mode="w", dir=dest_path.parent, delete=False, suffix=".tmp",
        )
        tmp_path = Path(tmp_file.name)
        try:
            tmp_file.write(payload)
            tmp_file.close()
            # NamedTemporaryFile always creates its file mode 0600, regardless
            # of the process umask. os.replace preserves that mode, so
            # without this the destination would silently end up owner-only
            # instead of the umask-derived mode a plain open()/write_text()
            # would have produced (relevant since this path is also used for
            # already-mounted filesystems read by other users/processes).
            umask = os.umask(0)
            os.umask(umask)
            os.chmod(tmp_path, 0o666 & ~umask)
            os.replace(tmp_path, dest_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        return str(dest_path)

    if parsed.scheme == "s3":
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        hook_kwargs = {"aws_conn_id": aws_conn_id} if aws_conn_id else {}
        aws_conn_id_used = aws_conn_id or "aws_default"
        try:
            s3_hook = S3Hook(**hook_kwargs)
            # S3Hook.__init__ (via AwsGenericHook) only assigns attributes; it
            # performs no connection lookup. The actual Airflow connection
            # resolution is a lazy cached_property that doesn't fire until the
            # first client use inside load_string() below. Trigger it here
            # instead, so a missing/misconfigured aws_conn_id raises inside
            # this try block (and gets wrapped below) rather than escaping
            # unwrapped from load_string().
            s3_hook.get_connection(aws_conn_id_used)
        except AirflowNotFoundException:
            # No Airflow Connection row defined for aws_conn_id_used. This is
            # the same case AwsGenericHook.conn_config handles internally
            # (it catches this exact exception and falls back to an empty
            # AwsConnectionWrapper), letting boto3's default credential chain
            # (IAM instance role, env vars, shared credentials profile) take
            # over. Don't raise here: let load_string() proceed and go
            # through conn_config's own graceful fallback.
            pass
        except Exception as e:
            raise AirflowException(
                f"AWS connection aws_conn_id='{aws_conn_id_used}' not found or misconfigured. "
                "Verify the Airflow AWS connection exists and is configured correctly."
            ) from e
        s3_hook.load_string(payload, key=key, bucket_name=bucket, replace=True)
        return report_dest

    raise ValueError(
        f"Unsupported report_dest scheme: {parsed.scheme!r} "
        "(expected a local path, file://, or s3://)."
    )
