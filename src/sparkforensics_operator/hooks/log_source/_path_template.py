"""
Checks on a path a log source was configured with, once Airflow has
rendered it (see hooks/_templating.py). Rendered values can come from
anything Jinja reaches, such as a run_id set through Airflow's trigger API
or an upstream task's XCom, so a rendered path may not climb out of its
intended directory with "..".
"""
from __future__ import annotations

import re
from pathlib import PurePosixPath

from sparkforensics_operator._compat import AirflowException

# The str.format placeholders path_template used to take before it became a
# Jinja template.
_OLD_PLACEHOLDER_RE = re.compile(r"\{(ds|run_id|dag_id|task_id|logical_date)(?::[^}]*)?\}")


def checked_path(path: str, what: str = "path_template") -> str:
    if "{{" in path or "{%" in path:
        raise AirflowException(
            f"{what} {path!r} was not rendered: SparkForensicsOperator renders it before "
            "locate(); a hook used on its own needs the rendered value."
        )
    old = _OLD_PLACEHOLDER_RE.search(path)
    if old:
        raise AirflowException(
            f"{what} {path!r} uses the {old.group(0)} placeholder, which is no longer "
            f"supported: {what} is a Jinja template now, so write {{{{ {old.group(1)} }}}} "
            "instead (for a date format, e.g. {{ logical_date.strftime('%Y-%m-%d') }})."
        )
    if not path.strip():
        raise AirflowException(f"{what} rendered to an empty path.")
    if ".." in PurePosixPath(path).parts:
        raise AirflowException(f"{what} rendered to {path!r}, which contains a '..' segment.")
    return path
