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
    assert pkg.SubprocessAnalyzeHook is not None
    assert pkg.Notifier is not None
    assert set(pkg.__all__) == {
        "SparkForensicsOperator",
        "spark_forensics_callback",
        "ThresholdBreached",
        "LogSourceHook",
        "HistoryServerLogSourceHook",
        "FilesystemLogSourceHook",
        "XComLogSourceHook",
        "SFTPLogSourceHook",
        "SSHTunneledLogSourceHook",
        "AnalyzeHook",
        "SubprocessAnalyzeHook",
        "Notifier",
    }
