from importlib.metadata import version

from sparkforensics_operator.callback import spark_forensics_callback
from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks.analyze.base import AnalyzeHook, DeferrableAnalyzeHook
from sparkforensics_operator.hooks.analyze.ssh import SSHAnalyzeHook
from sparkforensics_operator.hooks.analyze.subprocess import SubprocessAnalyzeHook
from sparkforensics_operator.hooks.log_source.base import LogSourceHook
from sparkforensics_operator.hooks.log_source.filesystem import FilesystemLogSourceHook
from sparkforensics_operator.hooks.log_source.history_server import HistoryServerLogSourceHook
from sparkforensics_operator.hooks.log_source.history_server_app import HistoryServerAppLogSourceHook
from sparkforensics_operator.hooks.log_source.remote_path import RemotePathLogSourceHook
from sparkforensics_operator.hooks.log_source.sftp import SFTPLogSourceHook
from sparkforensics_operator.hooks.log_source.tunnel import SSHTunneledLogSourceHook
from sparkforensics_operator.hooks.log_source.xcom import XComLogSourceHook
from sparkforensics_operator.log_ref import EventLogRef, HistoryServerApp, LocalEventLog, RemoteEventLog
from sparkforensics_operator.notify import Notifier
from sparkforensics_operator.operator import SparkForensicsOperator

__version__ = version("sparkforensics-operator")

__all__ = [
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
]
