"""
ReportLink turns the report destination into a clickable link on the task
in the Airflow UI. Where get_link runs differs between Airflow majors:

- Airflow 2.x: the webserver calls get_link on the deserialized operator
  every time the task page renders. The link class must be registered
  through a plugin (see plugin.py) or deserialization drops it. get_link
  reads the destination sinks.persist() wrote to from XCom (auto-pushed
  under "return_value" by execute()'s return value); the webserver has
  metadata-database access, so XCom.get_value is the supported read.
- Airflow 3.x: the task runner calls get_link once on the worker, after
  execute(), with the rendered task, and stores the result in XCom under
  self.xcom_key; the API server renders the link from that XCom. Workers
  must not read the metadata database, so get_link returns the
  destination execute() recorded on the operator after sinks.persist()
  succeeded, or "" when the run failed before persisting a report. Errors
  propagate to the task runner, which logs them in the task log.
"""
import logging

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS, BaseOperatorLink

log = logging.getLogger(__name__)

_XCOM_RETURN_KEY = "return_value"


class ReportLink(BaseOperatorLink):
    name = "SparkForensics report"

    def get_link(self, operator, *, ti_key) -> str:
        if AIRFLOW_V3_PLUS:
            return operator.persisted_report_dest or ""
        try:
            from airflow.models.xcom import XCom

            value = XCom.get_value(ti_key=ti_key, key=_XCOM_RETURN_KEY)
        except Exception:
            # Raising here would turn the webserver's extra-links request
            # into an HTTP 500; an empty link renders as a disabled button.
            log.exception("ReportLink could not read the report destination from XCom.")
            return ""
        return value or ""
