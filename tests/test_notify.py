from sparkforensics_operator.notify import Notifier
from sparkforensics_operator.report import Report


def _report():
    return Report(
        schema_version=3,
        summary={"impactBandCounts": {"critical": 1, "warning": 2, "info": 0}},
        findings=[], recommendations=[], clean_checks=[],
    )


class _RecordingNotifier(Notifier):
    def __init__(self):
        self.sent = []

    def _send(self, report, report_destination):
        self.sent.append((report, report_destination))


class _FailingNotifier(Notifier):
    def _send(self, report, report_destination):
        raise RuntimeError("connection refused")


def test_notify_delegates_to_send():
    notifier = _RecordingNotifier()
    report = _report()
    notifier.notify(report, "s3://reports/app-1.json")
    assert notifier.sent == [(report, "s3://reports/app-1.json")]


def test_notify_swallows_any_failure_and_never_raises():
    notifier = _FailingNotifier()
    notifier.notify(_report(), "s3://reports/app-1.json")  # must not raise


def test_default_message_includes_counts_and_destination():
    message = Notifier._default_message(_report(), "s3://reports/app-1.json")
    assert "1 critical" in message
    assert "2 warning" in message
    assert "s3://reports/app-1.json" in message


def _described_report(**summary):
    report = _report()
    report.summary = {"impactBandCounts": {"critical": 1, "warning": 2, "info": 0}, **summary}
    return report


def test_default_message_without_run_details_keeps_the_counts_and_destination_only():
    message = Notifier._default_message(_report(), "s3://reports/app-1.json")

    assert message == (
        "SparkForensics report: 1 critical, 2 warning, 0 info findings. "
        "Report: s3://reports/app-1.json"
    )


def test_default_message_names_the_app_verdict_and_run_time():
    report = _described_report(app={"name": "daily_customer_etl"}, runShape={"wallClockMs": 47688})
    report.verdict = {"title": "Start with Stage 7"}

    message = Notifier._default_message(report, "s3://reports/app-1.json")

    assert message == (
        "SparkForensics report for daily_customer_etl: Start with Stage 7. Ran for 47.7 s. "
        "1 critical, 2 warning, 0 info findings. Report: s3://reports/app-1.json"
    )


def test_default_message_mentions_skipped_lines_only_when_there_are_some():
    skipped = Notifier._default_message(_described_report(skippedLines=3), "/r.json")
    clean = Notifier._default_message(_described_report(skippedLines=0), "/r.json")

    assert "3 event log lines were skipped." in skipped
    assert "skipped" not in clean


def test_default_message_does_not_double_the_verdicts_full_stop():
    report = _described_report()
    report.verdict = {"title": "This run failed: its job did not finish."}

    assert "did not finish. 1 critical" in Notifier._default_message(report, "/r.json")


def test_default_message_writes_a_long_run_time_in_minutes_or_hours():
    minutes = _described_report(runShape={"wallClockMs": 125_000})
    hours = _described_report(runShape={"wallClockMs": 7_500_000})

    assert "Ran for 2 min 5 s." in Notifier._default_message(minutes, "/r.json")
    assert "Ran for 2 h 5 min." in Notifier._default_message(hours, "/r.json")
