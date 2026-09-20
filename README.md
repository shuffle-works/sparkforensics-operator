# sparkforensics-operator

[![CI](https://github.com/shuffle-works/sparkforensics-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/shuffle-works/sparkforensics-operator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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
- Three ways to get the event log: fetch it from the Spark History Server,
  read it from a filesystem/mounted-HDFS path template, or pull it straight
  out of XCom if the upstream task already has it.
- Budget thresholds mirror `sparkforensics-analyze`'s own CLI flags (max
  runtime, spill, skew, failed-task rate, min efficiency); only the ones
  you configure are enforced, and a breach raises `ThresholdBreached`
  without consuming the task's retries, since re-running would just reach
  the same verdict.
- Reports persist to a local path, `file://`, or `s3://`, and a clickable
  "SparkForensics report" link shows up on the task in the Airflow UI.
- An optional `Notifier` you implement (Slack, MS Teams, email, PagerDuty,
  ZenDuty, whatever you use) gets a best-effort pass/fail summary; a
  delivery failure never fails the task itself.
- Tested against both Airflow 2.6+ and Airflow 3.0+ (CI matrix, Python
  3.9-3.12). 97 tests, 96% coverage, no live Spark/Airflow/Node needed to
  run them: everything is mocked.

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
    log_source=FilesystemLogSourceHook(path_template="/mnt/spark-logs/{run_id}/eventlog"),
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

The worker needs Node.js (`>=22.18.0 <23.0.0 || >=23.6.0`) and the
`sparkforensics` npm package installed, so `sparkforensics-analyze` is
resolvable on `PATH`. Add the `s3` extra if `report_dest` is `s3://...`.
Full details in [`docs/runbook.md`](docs/runbook.md).

## Development

```bash
pip install -e ".[test,s3]"
pytest
```

Test against a specific Airflow major version:

```bash
tox -e py311-airflow2
tox -e py311-airflow3
```

## Learn more

- [Architecture](docs/architecture.md), components and design decisions
- [API reference](docs/api-reference.md), the full operator/hook/threshold
  surface
- [Runbook](docs/runbook.md), worker prerequisites and troubleshooting
- [Glossary](docs/glossary.md), domain terms

## License

[MIT](LICENSE)
