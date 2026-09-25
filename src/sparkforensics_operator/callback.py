from __future__ import annotations

import copy
import logging
from typing import Callable

from sparkforensics_operator.exceptions import ThresholdBreached
from sparkforensics_operator.hooks._templating import render_with_task_env
from sparkforensics_operator.operator import run_spark_forensics
from sparkforensics_operator.summary import validate_report_url_template

log = logging.getLogger("airflow.task")


def spark_forensics_callback(
    *,
    log_source,
    backend,
    report_dest: str,
    max_runtime_ms: int | None = None,
    max_spill_gb: float | None = None,
    max_skew_ratio: float | None = None,
    max_failed_task_rate_pct: float | None = None,
    min_efficiency_pct: float | None = None,
    on_threshold_breach: str = "fail",
    notifier=None,
    aws_conn_id: str | None = None,
    deferrable: bool = False,
    report_url_template: str | None = None,
) -> Callable[[dict], None]:
    """Builds an on_success_callback for the upstream Spark task, sharing
    SparkForensicsOperator's exact execute() logic via run_spark_forensics.

    Note (see the plan's "Global constraints"): Airflow itself catches and
    only logs any exception this callback raises, it never retries and
    never fails the upstream task. Set on_threshold_breach="fail" here only
    to get that log-and-continue behavior on breach; it will not fail the
    DAG the way the standalone-operator trigger shape does.

    A callback runs outside any task, so it cannot defer: deferrable=True
    raises ValueError. Use SparkForensicsOperator(deferrable=True) instead.

    Airflow renders no callback arguments, so the callback renders
    report_dest and the hooks' template_fields itself, with the upstream
    task's Jinja environment and the callback's context, the same values
    the operator would see. Strings are rendered as templates, never read as
    template files by the upstream task's template_ext. It renders copies:
    the hooks passed here are shared by every run of the upstream task.
    """
    if deferrable:
        raise ValueError(
            "spark_forensics_callback cannot defer: an on_success_callback runs outside "
            "any task, with no worker slot to free and no task to resume. Use "
            "SparkForensicsOperator(deferrable=True) as a downstream task instead."
        )
    if report_url_template is not None:
        validate_report_url_template(report_url_template)
    thresholds = {
        "max_runtime_ms": max_runtime_ms,
        "max_spill_gb": max_spill_gb,
        "max_skew_ratio": max_skew_ratio,
        "max_failed_task_rate_pct": max_failed_task_rate_pct,
        "min_efficiency_pct": min_efficiency_pct,
    }

    def _callback(context: dict) -> None:
        ti = context.get("ti")
        run_log_source, run_backend, run_report_dest = copy.copy(log_source), copy.copy(backend), report_dest
        task = context.get("task")
        if task is not None:
            run_log_source = render_with_task_env(task, run_log_source, context)
            run_backend = render_with_task_env(task, run_backend, context)
            run_report_dest = render_with_task_env(task, run_report_dest, context)
        try:
            destination = run_spark_forensics(
                context,
                log_source=run_log_source,
                backend=run_backend,
                report_dest=run_report_dest,
                thresholds=thresholds,
                on_threshold_breach=on_threshold_breach,
                notifier=notifier,
                log=log,
                aws_conn_id=aws_conn_id,
                report_url_template=report_url_template,
            )
        except ThresholdBreached as e:
            # on_threshold_breach="fail" (the default) makes run_spark_forensics
            # raise ThresholdBreached *after* the report has already been
            # persisted successfully. Without this, the callback path would
            # never record the report's location on the one path (a breach)
            # where a user most wants the report link. run_spark_forensics
            # attaches the already-known-good destination to the exception
            # for exactly this reason (see operator.py); still push it to
            # XCom before letting the breach propagate as usual.
            destination = getattr(e, "destination", None)
            if ti and destination is not None:
                ti.xcom_push(key="return_value", value=destination)
            raise

        # No operator auto-pushes this callback's return value to XCom, so
        # push it manually under the same key ("return_value") an operator's
        # own return value would use (see links.py's _XCOM_RETURN_KEY),
        # keeping the persisted report's location discoverable via
        # ti.xcom_pull() on the upstream task. Guard against a malformed/test
        # context missing "ti": this is a notification-adjacent side effect
        # and must not turn an already-successful run into a new failure.
        if ti:
            ti.xcom_push(key="return_value", value=destination)

    return _callback
