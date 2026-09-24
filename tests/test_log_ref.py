from pathlib import Path

from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog


def test_describe_names_where_each_log_lives():
    assert LocalEventLog(Path("/tmp/app.log")).describe() == "/tmp/app.log"
    assert RemoteEventLog("onprem_ssh", "/logs/app-1").describe() == "/logs/app-1 on ssh_conn_id='onprem_ssh'"
    assert HistoryServerApp("http://shs:18080", "app-1").describe() == "app app-1 on http://shs:18080"
    assert HistoryServerApp("http://shs:18080", "app-1", "2").describe() == "app app-1 attempt 2 on http://shs:18080"
