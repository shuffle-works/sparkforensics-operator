"""
ReportLink turns the persisted report into a clickable link on the task in
the Airflow UI: the browser URL built from SparkForensicsOperator's
report_url_template when one is set, else the raw report destination.
Where get_link runs differs between Airflow majors:

- Airflow 2.x: the webserver calls get_link on the deserialized operator
  every time the task page renders. The link class must be registered, or
  deserialization drops it; the package's provider metadata registers it
  (see get_provider_info.py). get_link reads the run's summary XCom
  (summary.SUMMARY_XCOM_KEY, pushed before a threshold breach raises, so it
  also exists for a failed run), falling back to the "return_value" XCom
  execute() returns; the webserver has metadata-database access, so
  XCom.get_value is the supported read.
- Airflow 3.x: the task runner calls get_link once on the worker, after
  execute(), with the rendered task, and stores the result in XCom under
  self.xcom_key; the API server renders the link from that XCom. Workers
  must not read the metadata database, so get_link returns what execute()
  recorded on the operator after sinks.persist() succeeded, or "" when the
  run failed before persisting a report. Errors propagate to the task
  runner, which logs them in the task log.
"""
import logging

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS, BaseOperatorLink
from sparkforensics_operator.summary import SUMMARY_XCOM_KEY

log = logging.getLogger(__name__)

_XCOM_RETURN_KEY = "return_value"


class ReportLink(BaseOperatorLink):
    name = "SparkForensics report"

    def get_link(self, operator, *, ti_key) -> str:
        if AIRFLOW_V3_PLUS:
            return operator.persisted_report_url or operator.persisted_report_dest or ""
        try:
            from airflow.models.xcom import XCom

            summary = XCom.get_value(ti_key=ti_key, key=SUMMARY_XCOM_KEY)
            if isinstance(summary, dict):
                return summary.get("report_url") or summary.get("destination") or ""
            value = XCom.get_value(ti_key=ti_key, key=_XCOM_RETURN_KEY)
        except Exception:
            # Raising here would turn the webserver's extra-links request
            # into an HTTP 500; an empty link renders as a disabled button.
            log.exception("ReportLink could not read the report destination from XCom.")
            return ""
        return value or ""
