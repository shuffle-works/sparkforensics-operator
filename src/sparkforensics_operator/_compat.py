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
except ImportError:  # Airflow 2.x has no airflow.sdk package
    from airflow.models.baseoperator import BaseOperator
    try:
        from airflow.models.baseoperatorlink import BaseOperatorLink  # Airflow >= 2.8
    except ImportError:  # Airflow 2.6/2.7
        from airflow.models.baseoperator import BaseOperatorLink

__all__ = ["BaseOperator", "BaseOperatorLink"]
