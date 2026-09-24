# API reference

## `SparkForensicsOperator`

```python
SparkForensicsOperator(
    task_id: str,
    log_source: LogSourceHook,
    backend: AnalyzeHook,
    report_dest: str,                        # local path, file://, or s3://; Jinja-templated
    max_runtime_ms: int | None = None,
    max_spill_gb: float | None = None,
    max_skew_ratio: float | None = None,
    max_failed_task_rate_pct: float | None = None,
    min_efficiency_pct: float | None = None,
    on_threshold_breach: str = "fail",        # "fail" | "warn" | "ignore"
    notifier: Notifier | None = None,
    aws_conn_id: str | None = None,           # non-default Airflow AWS connection for s3:// report_dest
    **base_operator_kwargs,
)
```

Returns (and auto-pushes to XCom as `return_value`) the `report_dest`
string it actually persisted to.

All public classes are importable from the top-level package, e.g.
`from sparkforensics_operator import SparkForensicsOperator, ThresholdBreached, LogSourceHook, AnalyzeHook, HistoryServerLogSourceHook`.

A threshold breach raises `ThresholdBreached` (an `AirflowFailException`), so
it fails the task immediately without consuming its configured retries: the
breach is a deterministic verdict from the completed analysis, retrying
would only re-run the same fetch+analyze cycle to reach the same result.

## `spark_forensics_callback`

Same keyword arguments as `SparkForensicsOperator` (minus `task_id` and any
`BaseOperator` kwargs). Returns a callable to attach as
`on_success_callback` on the upstream Spark task:

```python
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

Note: unlike `SparkForensicsOperator.report_dest` (a templated field, rendered
by Airflow before `execute()`), `spark_forensics_callback`'s `report_dest` is
a plain Python value evaluated once at DAG-parse time. Airflow does not
Jinja-render `on_success_callback` factory arguments in either major version,
so `{{ run_id }}`-style syntax here would be passed through literally, never
rendered. To vary `report_dest` per run, build the path from the callback's
own `context` argument instead:

```python
run_spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    on_success_callback=lambda context: spark_forensics_callback(
        log_source=XComLogSourceHook(task_id="run_spark_job"),
        backend=SubprocessAnalyzeHook(),
        report_dest=f"s3://reports/{context['run_id']}/app.json",
        max_runtime_ms=3_600_000,
    )(context),
    ...,
)
```

Caveat: an exception here is logged by Airflow and swallowed, it never
fails the upstream task and never retries. Use the standalone-operator
trigger shape (`spark_task >> SparkForensicsOperator(...)`) if a threshold
breach must fail the DAG.

Note: since no operator auto-pushes this callback's return value, the
callback pushes the persisted report's destination to XCom itself, under
key `return_value` on the *upstream* task it's attached to (the same key
an operator's own return value would use). It's discoverable via
`ti.xcom_pull(task_ids="run_spark_job")`, even though no `ReportLink` is
attached to that task by default.

## Event log references

`LogSourceHook.resolve(context)` returns an `EventLogRef` saying where the
log is, and `AnalyzeHook.analyze(log_ref, thresholds)` reads it from there.
All three kinds are frozen dataclasses importable from the top-level
package:

- `LocalEventLog(path: Path)`, a file or rolling-log directory on the
  Airflow worker.
- `RemoteEventLog(ssh_conn_id: str, path: str)`, a file or rolling-log
  directory on the host behind `ssh_conn_id`.
- `HistoryServerApp(base_url: str, app_id: str, attempt_id: str | None = None)`,
  a Spark History Server application the CLI fetches itself.
  `base_url` must be reachable from wherever the analysis runs.

`LogSourceHook.cleanup(log_ref)` receives the same reference after
analysis. A custom `LogSourceHook` implements `resolve()`; a custom
`AnalyzeHook` sets `supported_log_refs` and implements `_analyze()`.

Which log source works with which backend:

| log source | resolves to | `SubprocessAnalyzeHook` (worker) | `SSHAnalyzeHook` (SSH host) |
|---|---|---|---|
| `HistoryServerLogSourceHook` | `LocalEventLog` | yes | no |
| `FilesystemLogSourceHook` | `LocalEventLog` | yes | no |
| `XComLogSourceHook` | `LocalEventLog` | yes | no |
| `SFTPLogSourceHook` | `LocalEventLog` | yes | no |
| `SSHTunneledLogSourceHook` | `LocalEventLog` | yes | no |
| `RemotePathLogSourceHook` | `RemoteEventLog` | no | yes, same `ssh_conn_id` |
| `HistoryServerAppLogSourceHook` | `HistoryServerApp` | yes, `base_url` as seen from the worker | yes, `base_url` as seen from the SSH host |

An unsupported pairing raises `AirflowException` ("... cannot analyze
...; it reads: ...") from `analyze()` before the CLI runs.

## `LogSourceHook` implementations

- `HistoryServerLogSourceHook(base_url, app_id, attempt_id=None, dest_dir=None, timeout=300)`.
  No auth/session support today (a Kerberos/SPNEGO-protected History Server
  isn't reachable; fork `LogSourceHook` if you need this). `timeout` bounds
  the download's total wall-clock duration, not just a single read/connect;
  see the runbook's "exceeded {timeout}s" entry.
- `FilesystemLogSourceHook(path_template, dest_dir=None)`, `path_template`
  supports `{ds}`, `{run_id}`, `{dag_id}`, `{task_id}`, `{logical_date}`.
  `{ds}`, `{run_id}`, `{dag_id}`, and `{task_id}` are sanitized to their path
  basename (`Path(str(value)).name`) before rendering, so a crafted `run_id`
  (e.g. via `airflow dags trigger --run-id`) can't escape `path_template`'s
  intended directory via `../` segments; `{logical_date}` is not sanitized
  since it's a `datetime`, not an attacker-controlled string.
- `XComLogSourceHook(task_id, xcom_key="return_value")`
- `SFTPLogSourceHook(ssh_conn_id, path_template, dest_dir=None)`, the
  SSH-reachable equivalent of `FilesystemLogSourceHook` for an on-prem
  path the cloud worker can't mount. `path_template` supports the same
  `{ds}`/`{run_id}`/`{dag_id}`/`{task_id}`/`{logical_date}` substitutions.
  Unlike `FilesystemLogSourceHook`, every fetch is a remote-to-local copy.
  Same `cleanup()` convention as `HistoryServerLogSourceHook` (not
  `FilesystemLogSourceHook`): auto-cleaned when `dest_dir` is `None` (a
  private temp dir this hook owns), a no-op when `dest_dir` is set (a
  caller-managed shared directory).
- `SSHTunneledLogSourceHook(ssh_conn_id, remote_host, remote_port, hook_factory)`,
  tunnels network access to any HTTP-based `LogSourceHook` (e.g.
  `HistoryServerLogSourceHook`) through the same SSH connection an
  `SSHOperator` already uses. `hook_factory` receives the tunnel's local
  base URL (`http://127.0.0.1:<port>`) and must return a constructed
  `LogSourceHook`, e.g.
  `lambda base_url: HistoryServerLogSourceHook(base_url=base_url, app_id=...)`.
  Requires the `ssh` extra
  (`apache-airflow-providers-ssh>=5.0`, `apache-airflow-providers-sftp>=4.0`);
  note the `ssh` extra's own effective floor is `apache-airflow>=2.11`
  (transitively, via the ssh provider), higher than this package's overall
  `apache-airflow>=2.6` floor.
  The wrapped hook must fetch the log (return a `LocalEventLog`) while the
  tunnel is open; a hook that returns another reference kind is rejected.
  For a History Server reachable only from the SSH host, pairing
  `HistoryServerAppLogSourceHook` with `SSHAnalyzeHook` avoids downloading
  the log at all.
- `RemotePathLogSourceHook(ssh_conn_id, path_template)`, an event log that
  stays on the host behind `ssh_conn_id`, for `SSHAnalyzeHook` with the same
  `ssh_conn_id`. `path_template` supports the same sanitized substitutions
  as `FilesystemLogSourceHook`. It does no I/O: a missing path surfaces as
  the CLI's exit-2 error.
- `HistoryServerAppLogSourceHook(base_url, app_id, attempt_id=None)`, a
  History Server application passed to the CLI as
  `--shs-base-url`/`--app-id`/`--attempt-id` instead of being downloaded.
  `app_id`/`attempt_id` are plain values, as for
  `HistoryServerLogSourceHook`. Same no-auth limitation as
  `HistoryServerLogSourceHook`.

Each fetching hook's `resolve()`-returned path is cleaned up automatically
after analysis, but only when the hook created that path itself: see the
runbook's "Disk fills up" entry for exactly which configurations are (and
aren't) auto-cleaned.

## `AnalyzeHook` implementations

- `SubprocessAnalyzeHook(analyze_bin="sparkforensics-analyze", timeout=900)`,
  runs the CLI on the Airflow worker. Reads `LocalEventLog` and
  `HistoryServerApp`.
- `SSHAnalyzeHook(ssh_conn_id, analyze_bin="sparkforensics-analyze", timeout=900)`,
  runs the CLI on the host behind `ssh_conn_id` (the same Airflow SSH
  connection `SSHOperator` uses) and reads the JSON report from its stdout.
  Reads `RemoteEventLog` on the same `ssh_conn_id` and `HistoryServerApp`.
  `timeout` bounds the whole remote run in seconds, on the worker and on
  the SSH host through coreutils `timeout`. Every argument is
  shell-quoted. Requires the `ssh` extra on the worker, and coreutils
  `timeout`, Node.js 18+ and `sparkforensics-cli` on the SSH host; the
  worker needs none of them.

Both build the same CLI arguments, parse the same report and threshold
output, and treat exit codes the same way: 0, 1 and 3 produce a `Report`,
2 and anything else raise `AirflowException` with the CLI's stderr.

## Thresholds

Every threshold kwarg is optional; only configured ones are enforced.
Names mirror the `sparkforensics-analyze` CLI flags:

| kwarg | CLI flag | unit |
|---|---|---|
| `max_runtime_ms` | `--max-runtime` | milliseconds |
| `max_spill_gb` | `--max-spill` | gigabytes |
| `max_skew_ratio` | `--max-skew` | P95/median ratio |
| `max_failed_task_rate_pct` | `--max-failed-task-rate` | percent |
| `min_efficiency_pct` | `--min-efficiency` | percent |

## `Notifier`

This package ships no concrete notifier: maintaining channel integrations
(Slack, MS Teams, email, PagerDuty, ZenDuty, ...) is out of scope for a
Spark-log-analysis package. Implement `Notifier` for whatever channel you
use:

```python
from sparkforensics_operator import Notifier

class MyNotifier(Notifier):
    def _send(self, report, report_destination: str) -> None:
        ...  # deliver report/report_destination to your channel
```

`notifier.notify(report, report_destination)` is what `run_spark_forensics`
calls; it wraps your `_send()` so a delivery failure is logged as a
warning and never fails the task, whatever `_send()` raises. A
`Notifier._default_message(report, report_destination)` static helper
builds the same one-line plain-text summary the old built-in Slack
notifier used ("SparkForensics report: N critical, M warning, K info
findings. Report: {report_destination}"), if that's a reasonable starting
point for your channel's message body.
