"""
What makes a hook's arguments Jinja-templated: SparkForensicsOperator lists
log_source and backend in its template_fields, and Airflow renders an
object's own template_fields when it renders one of the operator's (nested
template rendering, Airflow 2 and 3 alike). So a hook only declares which
of its attributes are templates.
"""
from __future__ import annotations

from typing import Sequence


class TemplatedHookMixin:
    template_fields: Sequence[str] = ()

    def __repr__(self) -> str:
        # What the Airflow UI's "Rendered Template" view shows for the hook.
        fields = ", ".join(f"{name}={getattr(self, name, None)!r}" for name in self.template_fields)
        return f"{type(self).__name__}({fields})"
