import pytest

from sparkforensics_operator.hooks.log_source.base import LogSourceHook
from sparkforensics_operator.log_ref import LocalEventLog


def test_log_source_hook_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        LogSourceHook()


def test_log_source_hook_subclass_must_implement_resolve():
    class Incomplete(LogSourceHook):
        pass

    with pytest.raises(TypeError):
        Incomplete()


def test_cleanup_is_a_no_op_by_default(tmp_path):
    class Minimal(LogSourceHook):
        def locate(self, context):
            return LocalEventLog(tmp_path)

    Minimal().cleanup(LocalEventLog(tmp_path))  # must not raise

    assert tmp_path.exists()
