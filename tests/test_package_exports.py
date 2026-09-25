import sparkforensics_operator as pkg


def test_top_level_package_re_exports_the_public_surface():
    assert pkg.SparkForensicsOperator is not None
    assert pkg.spark_forensics_callback is not None
    assert pkg.ThresholdBreached is not None
    assert pkg.LogSourceHook is not None
    assert pkg.HistoryServerLogSourceHook is not None
    assert pkg.FilesystemLogSourceHook is not None
    assert pkg.XComLogSourceHook is not None
    assert pkg.SFTPLogSourceHook is not None
    assert pkg.SSHTunneledLogSourceHook is not None
    assert pkg.AnalyzeHook is not None
    assert pkg.DeferrableAnalyzeHook is not None
    assert pkg.SubprocessAnalyzeHook is not None
    assert pkg.SSHAnalyzeHook is not None
    assert pkg.RemotePathLogSourceHook is not None
    assert pkg.HistoryServerAppLogSourceHook is not None
    assert pkg.LocalEventLog is not None
    assert pkg.RemoteEventLog is not None
    assert pkg.HistoryServerApp is not None
    assert pkg.Notifier is not None
    assert set(pkg.__all__) == {
        "SparkForensicsOperator",
        "spark_forensics_callback",
        "ThresholdBreached",
        "LogSourceHook",
        "HistoryServerLogSourceHook",
        "HistoryServerAppLogSourceHook",
        "FilesystemLogSourceHook",
        "XComLogSourceHook",
        "SFTPLogSourceHook",
        "SSHTunneledLogSourceHook",
        "RemotePathLogSourceHook",
        "EventLogRef",
        "LocalEventLog",
        "RemoteEventLog",
        "HistoryServerApp",
        "AnalyzeHook",
        "DeferrableAnalyzeHook",
        "SubprocessAnalyzeHook",
        "SSHAnalyzeHook",
        "Notifier",
    }


def test_version_matches_installed_distribution():
    from importlib.metadata import version

    assert pkg.__version__ == version("sparkforensics-operator")
