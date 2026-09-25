"""
Isolates the one real Airflow-2-vs-3 import difference this package depends
on. Airflow 3's Task SDK moved BaseOperator/BaseOperatorLink to the
airflow.sdk namespace; airflow.sdk does not exist on Airflow 2.x. Everywhere
else in this package, import BaseOperator/BaseOperatorLink from here, never
straight from airflow.

Within the Airflow 2.x branch there is a second split: BaseOperatorLink
moved out of airflow.models.baseoperator and into its own
airflow.models.baseoperatorlink module in Airflow 2.8.0. On Airflow
2.6.0/2.7.0, airflow.models.baseoperatorlink does not exist yet and
BaseOperatorLink must still be imported from airflow.models.baseoperator.
"""
try:
    from airflow.sdk import BaseOperator, BaseOperatorLink

    AIRFLOW_V3_PLUS = True
except ImportError:  # Airflow 2.x has no airflow.sdk package
    AIRFLOW_V3_PLUS = False
    from airflow.models.baseoperator import BaseOperator
    try:
        from airflow.models.baseoperatorlink import BaseOperatorLink  # Airflow >= 2.8
    except ImportError:  # Airflow 2.6/2.7
        from airflow.models.baseoperator import BaseOperatorLink

# From Airflow 3.2 (Task SDK 1.2) the SDK has its own configuration and
# TaskDeferred; their Airflow 2 homes still work there but through the
# compatibility shims the SDK is retiring. Airflow 2.x and 3.0/3.1 only
# have the old homes.
try:
    from airflow.sdk.configuration import conf
except ImportError:
    from airflow.configuration import conf
try:
    from airflow.sdk.exceptions import TaskDeferred
except ImportError:
    from airflow.exceptions import TaskDeferred

__all__ = ["AIRFLOW_V3_PLUS", "BaseOperator", "BaseOperatorLink", "TaskDeferred", "conf"]
