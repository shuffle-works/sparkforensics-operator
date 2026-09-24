from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.base import AnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog


def test_analyze_hook_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        AnalyzeHook()


def test_analyze_hook_subclass_must_implement_analyze():
    class Incomplete(AnalyzeHook):
        pass

    with pytest.raises(TypeError):
        Incomplete()


class _LocalOnly(AnalyzeHook):
    supported_log_refs = (LocalEventLog,)

    def _analyze(self, log_ref, thresholds):
        return ("analyzed", log_ref, thresholds)


def test_analyze_delegates_a_supported_log_ref():
    log_ref = LocalEventLog(Path("/tmp/app.log"))

    assert _LocalOnly().analyze(log_ref, {"max_runtime_ms": 1}) == ("analyzed", log_ref, {"max_runtime_ms": 1})


def test_analyze_rejects_an_unsupported_log_ref_naming_what_it_reads():
    with pytest.raises(AirflowException, match="_LocalOnly cannot analyze HistoryServerApp.*it reads: LocalEventLog"):
        _LocalOnly().analyze(HistoryServerApp(base_url="http://shs:18080", app_id="app-1"), {})
