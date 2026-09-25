"""
Airflow provider metadata, returned through the apache_airflow_provider
entry point declared in pyproject.toml. Airflow's ProvidersManager reads it
to list this package under `airflow providers list` and, on Airflow 2.x, to
register ReportLink: Airflow 2.x keeps an operator link when it deserializes
a DAG only if a provider or plugin registered the link's class, and the
webserver renders from the serialized DAG. Airflow 3.x needs no link
registration.

Keep this module free of imports from the rest of the package: the
ProvidersManager can load it while Airflow itself is still importing.
"""
from __future__ import annotations

from importlib.metadata import version

_INTEGRATION = "SparkForensics"


def get_provider_info() -> dict:
    return {
        "package-name": "sparkforensics-operator",
        "name": _INTEGRATION,
        "description": (
            "Runs sparkforensics analysis on a Spark job's event log and acts on the "
            "report: persists it, pushes a summary to XCom, checks thresholds, notifies."
        ),
        "versions": [version("sparkforensics-operator")],
        "integrations": [
            {
                "integration-name": _INTEGRATION,
                "external-doc-url": "https://github.com/shuffle-works/sparkforensics-operator",
                "tags": ["software"],
            }
        ],
        "operators": [
            {
                "integration-name": _INTEGRATION,
                "python-modules": ["sparkforensics_operator.operator"],
            }
        ],
        "hooks": [
            {
                "integration-name": _INTEGRATION,
                "python-modules": [
                    "sparkforensics_operator.hooks.analyze.base",
                    "sparkforensics_operator.hooks.analyze.ssh",
                    "sparkforensics_operator.hooks.analyze.subprocess",
                    "sparkforensics_operator.hooks.log_source.base",
                    "sparkforensics_operator.hooks.log_source.filesystem",
                    "sparkforensics_operator.hooks.log_source.history_server",
                    "sparkforensics_operator.hooks.log_source.history_server_app",
                    "sparkforensics_operator.hooks.log_source.remote_path",
                    "sparkforensics_operator.hooks.log_source.sftp",
                    "sparkforensics_operator.hooks.log_source.tunnel",
                    "sparkforensics_operator.hooks.log_source.xcom",
                ],
            }
        ],
        "extra-links": ["sparkforensics_operator.links.ReportLink"],
    }
