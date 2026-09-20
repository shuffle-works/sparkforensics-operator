import pytest

from sparkforensics_operator.hooks.analyze.base import AnalyzeHook


def test_analyze_hook_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        AnalyzeHook()


def test_analyze_hook_subclass_must_implement_analyze():
    class Incomplete(AnalyzeHook):
        pass

    with pytest.raises(TypeError):
        Incomplete()
