from unittest.mock import patch

from sparkforensics_operator.hooks.log_source.history_server_app import HistoryServerAppLogSourceHook
from sparkforensics_operator.log_ref import HistoryServerApp


def test_locate_returns_a_history_server_app_without_downloading_anything():
    hook = HistoryServerAppLogSourceHook(base_url="http://localhost:18080/", app_id="app-1", attempt_id="2")

    with patch("requests.get") as get:
        log_ref = hook.locate({})

    get.assert_not_called()
    assert log_ref == HistoryServerApp(base_url="http://localhost:18080", app_id="app-1", attempt_id="2")
