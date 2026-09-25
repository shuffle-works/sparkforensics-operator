"""
The History Server hooks' app_id/attempt_id, once rendered: a Jinja
expression such as {{ ti.xcom_pull(...) }} renders a missing value as ""
or "None", which must not reach the History Server as an id.
"""
from __future__ import annotations

from sparkforensics_operator._compat import AirflowException

_MISSING = ("", "None")


def checked_app_id(app_id: str) -> str:
    if app_id is None or str(app_id).strip() in _MISSING:
        raise AirflowException(f"app_id rendered to {app_id!r}; expected a Spark application id.")
    return str(app_id)


def optional_attempt_id(attempt_id) -> str | None:
    if attempt_id is None or str(attempt_id).strip() in _MISSING:
        return None
    return str(attempt_id)
