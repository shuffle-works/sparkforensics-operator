from __future__ import annotations

from pathlib import Path


def _safe_path_component(value: object) -> str | None:
    """Reduce an attacker-influenceable context string (e.g. run_id, which is
    settable via Airflow's trigger API) to its filesystem basename before it
    is interpolated into path_template, the same way history_server.py
    sanitizes app_id. None is preserved as None rather than becoming the
    literal string "None"."""
    if value is None:
        return None
    return Path(str(value)).name


def _template_vars(context: dict) -> dict:
    dag = context.get("dag")
    task = context.get("task")
    return {
        "ds": _safe_path_component(context.get("ds")),
        "run_id": _safe_path_component(context.get("run_id")),
        # Not sanitized: logical_date is datetime-like and path_template may
        # apply a strftime format spec to it (e.g. "{logical_date:%Y-%m-%d}");
        # Path(...).name would break that formatting and, unlike a plain
        # string substitution, .format()'s format-spec mini-language for a
        # datetime doesn't accept arbitrary attacker strings.
        "logical_date": context.get("logical_date"),
        "dag_id": _safe_path_component(dag.dag_id) if dag is not None else None,
        "task_id": _safe_path_component(task.task_id) if task is not None else None,
    }
