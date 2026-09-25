"""
Isolates the Airflow-2-vs-3 import differences this package depends on.
Everywhere else in this package, import these Airflow symbols from here,
never straight from airflow; tests/test_compat.py checks that importing the
package emits no deprecation warning from its own modules.

Airflow 3's Task SDK moved BaseOperator/BaseOperatorLink to the airflow.sdk
namespace; airflow.sdk does not exist on Airflow 2.x. Within the Airflow
2.x branch there is a second split: BaseOperatorLink moved out of
airflow.models.baseoperator and into its own airflow.models.baseoperatorlink
module in Airflow 2.8.0. On Airflow 2.6.0/2.7.0,
airflow.models.baseoperatorlink does not exist yet and BaseOperatorLink
must still be imported from airflow.models.baseoperator.
"""
from __future__ import annotations

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

# The Task SDK re-homes more of Airflow's public API with each release:
# BaseHook from Airflow 3.1 (Task SDK 1.1), the exceptions, TaskDeferred and
# conf from 3.2 (Task SDK 1.2). Their Airflow 2 homes still work there, as
# the same objects, but through deprecation shims the SDK is retiring;
# Airflow 2.x and the earlier 3.x releases only have the old homes.
try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook
try:
    from airflow.sdk.exceptions import (
        AirflowException,
        AirflowFailException,
        AirflowNotFoundException,
        AirflowTaskTimeout,
        TaskDeferred,
    )
except ImportError:
    from airflow.exceptions import (
        AirflowException,
        AirflowFailException,
        AirflowNotFoundException,
        AirflowTaskTimeout,
        TaskDeferred,
    )
try:
    from airflow.sdk.configuration import conf
except ImportError:
    from airflow.configuration import conf


def current_context() -> dict | None:
    """The context of the task running in this process, or None outside
    one. Imported lazily: Airflow 2's home for it, airflow.operators.python,
    is costly to import."""
    try:
        from airflow.sdk import get_current_context
    except ImportError:
        from airflow.operators.python import get_current_context
    try:
        return get_current_context()
    except Exception:  # raised when no task is running
        return None


__all__ = [
    "AIRFLOW_V3_PLUS",
    "AirflowException",
    "AirflowFailException",
    "AirflowNotFoundException",
    "BaseHook",
    "BaseOperator",
    "BaseOperatorLink",
    "TaskDeferred",
    "conf",
    "current_context",
]
