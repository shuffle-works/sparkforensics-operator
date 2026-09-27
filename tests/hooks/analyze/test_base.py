from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.analyze.base import AnalyzeHook
from sparkforensics_operator.log_ref import HistoryServerApp, LocalEventLog, RemoteEventLog


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


class _ComparesLocal(_LocalOnly):
    supported_baseline_refs = (LocalEventLog,)

    def _analyze(self, log_ref, thresholds, baseline_ref=None):
        return ("analyzed", log_ref, thresholds, baseline_ref)


def test_analyze_passes_a_supported_baseline_ref():
    log_ref, baseline_ref = LocalEventLog(Path("/tmp/app-2.log")), LocalEventLog(Path("/tmp/app-1.log"))

    assert _ComparesLocal().analyze(log_ref, {}, baseline_ref=baseline_ref) == ("analyzed", log_ref, {}, baseline_ref)


def test_analyze_without_a_baseline_calls_a_backend_that_cannot_compare_as_before():
    log_ref = LocalEventLog(Path("/tmp/app.log"))

    assert _LocalOnly().analyze(log_ref, {}, baseline_ref=None) == ("analyzed", log_ref, {})


def test_a_backend_without_baseline_support_rejects_a_baseline():
    with pytest.raises(AirflowException, match="_LocalOnly cannot compare a run against a baseline"):
        _LocalOnly().analyze(LocalEventLog(Path("/tmp/app-2.log")), {}, baseline_ref=LocalEventLog(Path("/tmp/app-1.log")))


def test_a_history_server_app_is_never_a_baseline():
    baseline = HistoryServerApp(base_url="http://shs:18080", app_id="app-1")

    with pytest.raises(AirflowException, match="History Server application cannot be a baseline") as e:
        _ComparesLocal().analyze(LocalEventLog(Path("/tmp/app-2.log")), {}, baseline_ref=baseline)
    assert "resolves to LocalEventLog" in str(e.value)


def test_a_baseline_the_backend_cannot_read_names_what_it_reads():
    with pytest.raises(AirflowException, match="cannot use RemoteEventLog.*reads a baseline only as LocalEventLog"):
        _ComparesLocal().analyze(
            LocalEventLog(Path("/tmp/app-2.log")), {}, baseline_ref=RemoteEventLog("onprem_ssh", "/logs/app-1")
        )
