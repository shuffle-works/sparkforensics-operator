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
