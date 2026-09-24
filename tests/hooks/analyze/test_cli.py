from pathlib import Path

import pytest

from sparkforensics_operator.hooks.analyze._cli import build_cli_args
from sparkforensics_operator.log_ref import LocalEventLog


def test_build_cli_args_writes_to_stdout_without_an_out_path():
    args = build_cli_args("sparkforensics-analyze", LocalEventLog(Path("/tmp/app.log")), {"max_spill_gb": 2})

    assert args == ["sparkforensics-analyze", "/tmp/app.log", "--format", "json", "--max-spill", "2"]


def test_build_cli_args_rejects_something_that_is_not_an_event_log_reference():
    with pytest.raises(TypeError, match="Not an event log reference"):
        build_cli_args("sparkforensics-analyze", Path("/tmp/app.log"), {})
