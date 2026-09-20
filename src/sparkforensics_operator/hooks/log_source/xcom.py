from pathlib import Path

from airflow.exceptions import AirflowException

from .base import LogSourceHook


class XComLogSourceHook(LogSourceHook):
    """Reads a local event-log path that the upstream Spark task already
    pushed to XCom (e.g. it wrote the log to a shared/mounted path and
    pushed that path as its return value)."""

    def __init__(self, task_id: str, xcom_key: str = "return_value"):
        super().__init__()
        self.task_id = task_id
        self.xcom_key = xcom_key

    def fetch(self, context: dict) -> Path:
        value = context["ti"].xcom_pull(task_ids=self.task_id, key=self.xcom_key)
        if not value:
            raise AirflowException(
                f"No XCom value found for task_id={self.task_id!r} key={self.xcom_key!r}."
            )
        path = Path(value)
        if not path.exists():
            raise AirflowException(f"XCom-provided event log path does not exist: {path}")
        return path
