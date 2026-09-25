"""
What makes a hook's arguments Jinja-templated: SparkForensicsOperator lists
log_source and backend in its template_fields, and Airflow renders an
object's own template_fields when it renders one of the operator's (nested
template rendering, Airflow 2 and 3 alike). So a hook only declares which
of its attributes are templates.
"""
from __future__ import annotations

import copy
from typing import Any, Sequence


class TemplatedHookMixin:
    template_fields: Sequence[str] = ()

    def __repr__(self) -> str:
        # What the Airflow UI's "Rendered Template" view shows for the hook.
        fields = ", ".join(f"{name}={getattr(self, name, None)!r}" for name in self.template_fields)
        return f"{type(self).__name__}({fields})"


def task_renderer(task):
    """task, or a copy of it, whose render_template() renders a value, and a
    hook's template_fields in place, with task's Jinja environment (its
    DAG's macros, filters and user_defined_macros). Strings are always
    rendered as templates, never read as template files: the task's
    template_ext names files for its own template fields, and an upstream
    task's (".json" on EMR operators) would otherwise turn a report_dest
    such as "s3://.../app.json" into a template-file lookup. Only a task
    with a template_ext is copied."""
    if not task.template_ext:
        return task
    renderer = copy.copy(task)
    renderer.template_ext = ()
    return renderer


def render_with_task_env(task, value: Any, context: dict) -> Any:
    """Renders value with task_renderer(task)."""
    return task_renderer(task).render_template(value, context)
