"""
ReportLink reads the destination sinks.persist() wrote to (auto-pushed to
XCom under "return_value" by execute()'s return value) and turns it into a
clickable link on the task in the Airflow UI.

Reading an XCom by ti_key from an operator link is NOT part of Airflow 3's
declared airflow.sdk public interface, it reaches into airflow.models,
which is internal and can change between Airflow releases without notice
(see the plan's "Global constraints" section for the exact APIs this
mirrors). get_link must therefore never raise: any failure degrades to an
empty/disabled link rather than breaking the UI. The CI matrix (Task 17)
is what actually catches this breaking on a future Airflow release.
"""
import logging

from sparkforensics_operator._compat import BaseOperatorLink

log = logging.getLogger(__name__)

_XCOM_RETURN_KEY = "return_value"


def _xcom_module():
    import airflow.models.xcom as xcom

    return xcom


class ReportLink(BaseOperatorLink):
    name = "SparkForensics report"

    def get_link(self, operator, *, ti_key) -> str:
        try:
            xcom = _xcom_module()
            if hasattr(xcom, "XComModel"):  # Airflow 3.x
                row = xcom.XComModel.get_many(
                    key=_XCOM_RETURN_KEY,
                    run_id=ti_key.run_id,
                    dag_ids=ti_key.dag_id,
                    task_ids=ti_key.task_id,
                    map_indexes=ti_key.map_index,
                ).first()
                value = xcom.XComModel.deserialize_value(row) if row is not None else None
            else:  # Airflow 2.x
                value = xcom.XCom.get_value(ti_key=ti_key, key=_XCOM_RETURN_KEY)
            return value or ""
        except Exception:
            log.warning("ReportLink could not read the report destination from XCom.", exc_info=True)
            return ""
