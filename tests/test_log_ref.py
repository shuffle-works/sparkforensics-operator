import json
from pathlib import Path

import pytest

from sparkforensics_operator.log_ref import (
    HistoryServerApp,
    LocalEventLog,
    RemoteEventLog,
    log_ref_from_dict,
    log_ref_to_dict,
)


def test_describe_names_where_each_log_lives():
    assert LocalEventLog(Path("/tmp/app.log")).describe() == "/tmp/app.log"
    assert RemoteEventLog("onprem_ssh", "/logs/app-1").describe() == "/logs/app-1 on ssh_conn_id='onprem_ssh'"
    assert HistoryServerApp("http://shs:18080", "app-1").describe() == "app app-1 on http://shs:18080"
    assert HistoryServerApp("http://shs:18080", "app-1", "2").describe() == "app app-1 attempt 2 on http://shs:18080"


@pytest.mark.parametrize(
    "log_ref",
    [
        LocalEventLog(Path("/tmp/app.log")),
        RemoteEventLog("onprem_ssh", "/logs/app-1"),
        HistoryServerApp("http://shs:18080", "app-1"),
        HistoryServerApp("http://shs:18080", "app-1", "2"),
    ],
)
def test_log_refs_round_trip_through_json(log_ref):
    assert log_ref_from_dict(json.loads(json.dumps(log_ref_to_dict(log_ref)))) == log_ref


def test_log_ref_serialization_rejects_anything_else():
    with pytest.raises(TypeError, match="Not an event log reference"):
        log_ref_to_dict(Path("/tmp/app.log"))
    with pytest.raises(ValueError, match="Not a serialized event log reference"):
        log_ref_from_dict({"kind": "s3"})
