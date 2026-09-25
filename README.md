# sparkforensics-operator

[![CI](https://github.com/shuffle-works/sparkforensics-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/shuffle-works/sparkforensics-operator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/shuffle-works/sparkforensics-operator/blob/main/LICENSE)

An Airflow operator that runs [sparkforensics](https://github.com/shuffle-works/sparkforensics)
analysis on a Spark job's event log right after it finishes, then acts on
the result: persist the report, push it to XCom, notify (via any
messaging/paging system you implement), and optionally fail the DAG if a
budget was breached.

Airflow tells you a Spark task succeeded. It doesn't tell you that stage 14
spilled 40GB from a skewed join, or that runtime crept 20% past last week's
run. This package closes that gap without a separate monitoring job: it
runs as part of the DAG, right where the Spark task just ran.

## Why this exists

- Two ways to wire it in: a standalone `SparkForensicsOperator` that fails
  the DAG on a threshold breach, or a `spark_forensics_callback` factory to
  attach as `on_success_callback` on the Spark task itself, log-only.
- Where the log comes from and where the analysis runs are separate,
  composable choices. Fetch the log to the worker (Spark History Server,
  filesystem/mounted-HDFS path template, XCom, SFTP, or a History Server
  behind an SSH tunnel), or leave it where it is and point the analysis at
  it (a path on an SSH host, or a History Server application the CLI
  fetches itself).
- Run the analysis on the Airflow worker, or on an SSH host that already
  sees the logs, such as the node running the Spark History Server. With
  remote analysis the log never crosses the network, and the workers
  (including managed ones like MWAA) don't need Node.js. With
  `deferrable=True` the task also gives up its worker slot while the
  remote analysis runs, and a triggerer waits on it instead.
- Budget thresholds mirror `sparkforensics-analyze`'s own CLI flags (max
  runtime, spill, skew, failed-task rate, min efficiency); only the ones
  you configure are enforced, and a breach raises `ThresholdBreached`
  without consuming the task's retries, since re-running would just reach
  the same verdict.
- Reports persist to a local path, `file://`, or `s3://`, and a clickable
  "SparkForensics report" link shows up on the task in the Airflow UI. Set
  `report_url_template` (an S3 console URL, say) to make it open in a
  browser.
- A small summary (finding counts per impact band, breached thresholds,
  the destination) goes to XCom under `sparkforensics_summary`, so
  downstream tasks can branch without reading the report.
- Hook arguments are Jinja templates (`path_template="/logs/{{ run_id }}"`,
  `app_id="{{ ti.xcom_pull(...) }}"`), rendered per task like any
  templated operator field.
- Installs as an Airflow provider, listed by `airflow providers list`.
- An optional `Notifier` you implement (Slack, MS Teams, email, PagerDuty,
  ZenDuty, whatever you use) gets a best-effort pass/fail summary; a
  delivery failure never fails the task itself.
- Tested against both Airflow 2.6+ and Airflow 3.0+ (CI matrix, Python
  3.9-3.12). No live Spark, Airflow or Node is needed to run the tests:
  everything but the shell on the stand-in SSH host is mocked.

## Quick start

```bash
pip install sparkforensics-operator
```

Standalone operator, chained after the Spark task, fails the DAG on breach:

```python
from sparkforensics_operator import FilesystemLogSourceHook, SparkForensicsOperator, SubprocessAnalyzeHook

run_spark_job = SparkSubmitOperator(task_id="run_spark_job", ...)

check_spark_job = SparkForensicsOperator(
    task_id="check_spark_job",
    log_source=FilesystemLogSourceHook(path_template="/mnt/spark-logs/{{ run_id }}/eventlog"),
    backend=SubprocessAnalyzeHook(),
    report_dest="s3://reports/{{ run_id }}/report.json",
    max_runtime_ms=3_600_000,
    max_skew_ratio=3,
    on_threshold_breach="fail",
)

run_spark_job >> check_spark_job
```

Or attach it as a callback instead. Airflow logs and swallows any exception
raised inside a callback, so a breach here never fails or retries the
upstream task, whatever `on_threshold_breach` is set to:

```python
from sparkforensics_operator import SubprocessAnalyzeHook, XComLogSourceHook, spark_forensics_callback

run_spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    on_success_callback=spark_forensics_callback(
        log_source=XComLogSourceHook(task_id="run_spark_job"),
        backend=SubprocessAnalyzeHook(),
        report_dest="s3://reports/run_spark_job/app.json",
        max_runtime_ms=3_600_000,
    ),
    ...,
)
```

Or run the analysis on the Spark History Server's own node over SSH, so the
event log never reaches the worker. `ssh_conn_id` is the Airflow SSH
connection to that node, and `base_url` is the History Server as seen from
it:

```python
from sparkforensics_operator import HistoryServerAppLogSourceHook, SparkForensicsOperator, SSHAnalyzeHook

check_spark_job = SparkForensicsOperator(
    task_id="check_spark_job",
    log_source=HistoryServerAppLogSourceHook(
        base_url="http://localhost:18080",
        app_id="{{ ti.xcom_pull(task_ids='run_spark_job', key='app_id') }}",
    ),
    backend=SSHAnalyzeHook(ssh_conn_id="shs_node"),
    report_dest="s3://reports/{{ run_id }}/report.json",
    max_runtime_ms=3_600_000,
)
```

`RemotePathLogSourceHook(ssh_conn_id="shs_node", path_template="/spark-logs/{{ run_id }}")`
points `SSHAnalyzeHook` at an event log path on that host instead.

Add `deferrable=True` to that operator to release the worker slot while
the analysis runs: the CLI runs as a detached job on the SSH host and the
triggerer polls it, which needs a running triggerer with
`sparkforensics-operator[ssh]` installed. See
[Deferrable execution](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/api-reference.md#deferrable-execution).

Whichever host runs the analysis (the worker for `SubprocessAnalyzeHook`,
the SSH host for `SSHAnalyzeHook`) needs Node.js 18+ and the
`sparkforensics-cli` npm package (`npm install -g sparkforensics-cli`), so
`sparkforensics-analyze` is resolvable on `PATH`. Add the `ssh` extra for
the SSH hooks, and the `s3` extra if `report_dest` is `s3://...`.
Full details in [`docs/runbook.md`](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/runbook.md).

## Development

```bash
pip install -e ".[test,s3,ssh]"
pytest
```

Test against a specific Airflow major version:

```bash
tox -e py311-airflow2
tox -e py311-airflow3
```

## Learn more

- [Architecture](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/architecture.md), components and design decisions
- [API reference](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/api-reference.md), the full operator/hook/threshold
  surface
- [Runbook](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/runbook.md), worker and SSH host prerequisites, troubleshooting
- [Glossary](https://github.com/shuffle-works/sparkforensics-operator/blob/main/docs/glossary.md), domain terms

## License

[MIT](https://github.com/shuffle-works/sparkforensics-operator/blob/main/LICENSE)
