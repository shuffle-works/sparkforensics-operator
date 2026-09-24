"""
Registers ReportLink with Airflow through the airflow.plugins entry point
declared in pyproject.toml.

Airflow 2.x serializes an operator link by its class path and, when
deserializing, keeps it only if that class is a registered plugin or
provider link; otherwise it logs "Operator Link class ... not registered"
and drops the link, and the webserver (which renders from the serialized
DAG) never shows it. Airflow 3.x serializes links by name and XCom key and
needs no registration; ReportLink.operators is empty, so this plugin adds
no link there.
"""
from airflow.plugins_manager import AirflowPlugin

from sparkforensics_operator.links import ReportLink


class SparkForensicsPlugin(AirflowPlugin):
    name = "sparkforensics_operator"
    operator_extra_links = [ReportLink()]
