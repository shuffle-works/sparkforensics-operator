"""Renders a hook the way Airflow does for a real task: as a nested
template field of SparkForensicsOperator, through the operator's own
render_template_fields()."""
from sparkforensics_operator.hooks.analyze.base import AnalyzeHook
from sparkforensics_operator.operator import SparkForensicsOperator


class _Untemplated:
    """Fills the operator's other hook slot; Airflow skips objects without
    template_fields."""


def render(hook, **context):
    is_backend = isinstance(hook, AnalyzeHook)
    op = SparkForensicsOperator(
        task_id="render",
        log_source=_Untemplated() if is_backend else hook,
        backend=hook if is_backend else _Untemplated(),
        report_dest="/tmp/report.json",
        deferrable=False,
    )
    op.render_template_fields(context)
    return op.backend if is_backend else op.log_source
