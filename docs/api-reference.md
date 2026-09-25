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
    deferrable: bool | None = None,           # see "Deferrable execution" below
    report_url_template: str | None = None,   # see "Summary XCom and report URL" below
    **base_operator_kwargs,
)
```

Returns (and auto-pushes to XCom as `return_value`) the `report_dest`
string it actually persisted to.

`template_fields` is `("report_dest", "log_source", "backend")`: Airflow
renders `report_dest` and every templated argument of the two hooks (see
"Templated hook arguments" below) before `execute()`.

### Summary XCom and report URL

Every run with a task instance also pushes a summary under the XCom key
`sparkforensics_summary` (`SUMMARY_XCOM_KEY` in
`sparkforensics_operator.summary`), before notifying and before a
threshold breach raises. It is a dict:

| key | value |
|---|---|
| `schema_version` | the report's schema version |
| `destination` | where the report was persisted |
| `report_url` | the rendered `report_url_template`, or `None` |
| `impact_band_counts` | `{"critical": n, "warning": n, "info": n}` |
| `violated` | `True` if a threshold was violated (a violation line or CLI exit 1) |
| `breached_thresholds` | names of violated thresholds |
| `inconclusive_thresholds` | names of thresholds the CLI could not evaluate |
| `exit_code` | the CLI's exit code |

A deferred run pushes the same summary. `return_value` stays the
destination string.

`report_url_template` makes `ReportLink` open a browser URL instead of the
raw destination. It is a `str.format` template with four placeholders,
each filled from the persisted destination and percent-encoded (`/`
kept):

- `{destination}`, the whole destination.
- `{bucket}`, the host part of a URI such as `s3://bucket/...`; empty for
  a local path or `file://`.
- `{key}`, the path after the bucket, without its leading `/`; empty for
  a local path or `file://`.
- `{path}`, the path part (a local path as given).

```python
report_url_template="https://s3.console.aws.amazon.com/s3/object/{bucket}?prefix={key}"
```

The template must build an `http://` or `https://` URL and use only those
placeholders; anything else raises `ValueError` when the DAG is parsed.
Nothing is presigned: the link opens with the viewer's own access.

### Deferrable execution

`deferrable=True` runs the analysis as a detached job on the SSH host and
defers the task until it finishes, so no worker slot is held while the CLI
runs. It needs a backend that can run detached, a `DeferrableAnalyzeHook`:
`SSHAnalyzeHook` is the only one shipped. Any other backend with
`deferrable=True` raises `ValueError` when the DAG is parsed.

Left at `None`, `deferrable` follows Airflow's `[operators]
default_deferrable` setting, but only for a backend that can defer: with
`default_deferrable = True`, a `SubprocessAnalyzeHook` task still runs
synchronously instead of failing. `deferrable=False` always runs
synchronously.

A deferred run produces the same persisted report, threshold results,
`ReportLink`, XCom and notifier calls as a synchronous one, and raises the
same errors for the same outcome (exit codes, missing binary, timeout,
unparseable report, SSH failures). The deferral itself needs a running
triggerer; see the runbook's "Deferred runs" section for what else the
deployment needs and how retries, timeouts and clears behave.

```python
SparkForensicsOperator(
    task_id="forensics",
    log_source=RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/spark-events/{{ run_id }}"),
    backend=SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=1800),
    report_dest="s3://reports/{{ run_id }}/app.json",
    max_spill_gb=10,
    deferrable=True,
)
```

All public classes are importable from the top-level package, e.g.
`from sparkforensics_operator import SparkForensicsOperator, ThresholdBreached, LogSourceHook, AnalyzeHook, HistoryServerLogSourceHook`.

A threshold breach raises `ThresholdBreached` (an `AirflowFailException`), so
it fails the task immediately without consuming its configured retries: the
breach is a deterministic verdict from the completed analysis, retrying
would only re-run the same fetch+analyze cycle to reach the same result.

## `spark_forensics_callback`

Same keyword arguments as `SparkForensicsOperator` (minus `task_id` and any
`BaseOperator` kwargs). A callback cannot defer: `deferrable=True` raises
`ValueError`. Returns a callable to attach as
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

Airflow does not render callback arguments, so the callback renders
them itself when it runs: `report_dest` and the templated arguments of
`log_source` and `backend`, with the upstream task's Jinja environment
(its DAG's macros and filters) and the callback's context. Every value
is rendered as a template string, never loaded as a template file, so a
`report_dest` ending in `.json` works on an upstream task whose
`template_ext` includes `.json` (EMR, Spark on Kubernetes). `{{ run_id }}` in
`report_dest` works as it does on the operator. The hooks are rendered as
copies, so the instances passed to the factory stay templates for the
next run. `report_url_template` is not rendered: it is filled from the
destination only.

Caveat: an exception here is logged by Airflow and swallowed, it never
fails the upstream task and never retries. Use the standalone-operator
trigger shape (`spark_task >> SparkForensicsOperator(...)`) if a threshold
breach must fail the DAG.

Note: since no operator auto-pushes this callback's return value, the
callback pushes the persisted report's destination to XCom itself, under
key `return_value` on the *upstream* task it's attached to (the same key
an operator's own return value would use), along with the
`sparkforensics_summary` XCom. Both are discoverable via
`ti.xcom_pull(task_ids="run_spark_job", key=...)`, even though no
`ReportLink` is attached to that task by default.

## Event log references

`LogSourceHook.locate(context)` returns an `EventLogRef` saying where the
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
analysis. A custom `LogSourceHook` implements `locate()`; a custom
`AnalyzeHook` sets `supported_log_refs` and implements `_analyze()`. In a
deferred run, `cleanup()` is called on the fresh operator instance that
resumes the task, so it can't rely on state `locate()` left on the hook.

`log_ref_to_dict(log_ref)` and `log_ref_from_dict(data)` (in
`sparkforensics_operator.log_ref`) convert a reference to and from plain
JSON values; the deferrable mode uses them to carry it across the
deferral. They accept `RemoteEventLog` and `HistoryServerApp`, the
references a deferrable backend reads.

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
  is a Jinja template, such as `/spark-events/{{ run_id }}`. A rendered
  path containing a `..` segment is rejected, so a crafted `run_id` (say,
  from `airflow dags trigger --run-id`) or XCom value can't escape the
  intended directory.
- `XComLogSourceHook(task_id, xcom_key="return_value")`
- `SFTPLogSourceHook(ssh_conn_id, path_template, dest_dir=None)`, the
  SSH-reachable equivalent of `FilesystemLogSourceHook` for an on-prem
  path the cloud worker can't mount. `path_template` is Jinja, checked the
  same way.
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
  (`apache-airflow-providers-ssh>=6.0.1`, `apache-airflow-providers-sftp>=4.0`);
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
  `ssh_conn_id`. `path_template` is Jinja, checked as for
  `FilesystemLogSourceHook`. It does no I/O: a missing path surfaces as
  the CLI's exit-2 error.
- `HistoryServerAppLogSourceHook(base_url, app_id, attempt_id=None)`, a
  History Server application passed to the CLI as
  `--shs-base-url`/`--app-id`/`--attempt-id` instead of being downloaded.
  Same no-auth limitation as `HistoryServerLogSourceHook`.

### Templated hook arguments

Hook arguments are Jinja templates. Airflow renders them before
`execute()`, as nested template fields of `SparkForensicsOperator`, with
the task's context (`{{ run_id }}`, `{{ ds }}`, `{{ params.x }}`,
`{{ ti.xcom_pull(...) }}`, macros). Each hook class lists its templated
attributes in `template_fields`:

| hook | `template_fields` |
|---|---|
| `FilesystemLogSourceHook` | `path_template`, `dest_dir` |
| `SFTPLogSourceHook` | `ssh_conn_id`, `path_template`, `dest_dir` |
| `RemotePathLogSourceHook` | `ssh_conn_id`, `path_template` |
| `XComLogSourceHook` | `task_id`, `xcom_key` |
| `HistoryServerLogSourceHook` | `base_url`, `app_id`, `attempt_id`, `dest_dir` |
| `HistoryServerAppLogSourceHook` | `base_url`, `app_id`, `attempt_id` |
| `SSHTunneledLogSourceHook` | `ssh_conn_id`, `remote_host` |
| `SubprocessAnalyzeHook` | `analyze_bin` |
| `SSHAnalyzeHook` | `ssh_conn_id`, `analyze_bin`, `remote_base_dir` |

Each task renders its own copy, so one hook instance can be shared by
several tasks. Rendered values are checked when used:

- a path must be rendered (no `{{` left), non-empty and without a `..`
  segment;
- the old `{run_id}`-style placeholders are rejected, with an error that
  names the Jinja form;
- an `app_id` that rendered to `""` or `"None"` (as a missing XCom does)
  raises, and an `attempt_id` that did is treated as unset;
- a templated `remote_base_dir` is checked once rendered.

For `SSHTunneledLogSourceHook`, the hook `hook_factory` returns is
rendered too, at `locate()` time, with the task's context. A custom hook
subclass adds its own attribute names to `template_fields`. The
log-source method is `locate()`: Airflow 3's templater calls `resolve()`
on any template field that has one, so a custom hook must not define a
`resolve` method.

Each fetching hook's `locate()`-returned path is cleaned up automatically
after analysis, but only when the hook created that path itself: see the
runbook's "Disk fills up" entry for exactly which configurations are (and
aren't) auto-cleaned.

## `AnalyzeHook` implementations

- `SubprocessAnalyzeHook(analyze_bin="sparkforensics-analyze", timeout=900)`,
  runs the CLI on the Airflow worker. Reads `LocalEventLog` and
  `HistoryServerApp`.
- `SSHAnalyzeHook(ssh_conn_id, analyze_bin="sparkforensics-analyze", timeout=900, remote_base_dir=None, poll_interval=5)`,
  runs the CLI on the host behind `ssh_conn_id` (the same Airflow SSH
  connection `SSHOperator` uses) and reads the JSON report from its stdout.
  Reads `RemoteEventLog` on the same `ssh_conn_id` and `HistoryServerApp`.
  `timeout` bounds the whole remote run in seconds, on the worker and on
  the SSH host through coreutils `timeout`. Every argument is
  shell-quoted. Requires the `ssh` extra on the worker, and coreutils
  `timeout`, Node.js 18+ and `sparkforensics-cli` on the SSH host; the
  worker needs none of them.
  With `SparkForensicsOperator(deferrable=True)`, the CLI runs as a
  detached job instead, writing its report to a file under
  `remote_base_dir/<task instance>/<job>/`, and the triggerer checks on it
  every `poll_interval` seconds. `remote_base_dir` defaults to
  `$HOME/.sparkforensics/jobs` of the SSH user; set it to an absolute path
  without `$`, `` ` ``, `"`, `\`, `..` or control characters (a
  `ValueError` otherwise). The deferred mode also needs bash on the SSH
  host. Both modes use `setsid` and `pkill` there when present to stop a
  run. A synchronous run writes nothing under `remote_base_dir`.

`SparkForensicsOperator.on_kill()` stops the analysis when the task is
killed while a worker runs it: a deferrable task's backend abandons the
task instance's remote job (`DeferrableAnalyzeHook.abandon`), a
synchronous one calls `AnalyzeHook.on_kill()`, a no-op by default.
`SSHAnalyzeHook.on_kill()` closes the SSH channel and stops the remote CLI
by the pid it reported, if it had reported one yet.
Airflow runs no worker process for a deferred task, so killing one then
reaches its job only through the next try's sweep.

Both build the same CLI arguments, parse the same report and threshold
output, and treat exit codes the same way: 0, 1 and 3 produce a `Report`,
2 and anything else raise `AirflowException` with the CLI's stderr.

A custom backend that can run detached subclasses `DeferrableAnalyzeHook`
(an `AnalyzeHook`, exported from the top-level package) and implements, on
top of `_analyze()`:

- `submit(log_ref, thresholds, context) -> dict`, start the job and return
  it as JSON-native values; first stop and remove anything an earlier try
  of the same task instance left behind.
- `trigger_for(job)`, the trigger to defer on.
- `defer_timeout(job) -> timedelta`, how long to wait before giving up.
- `collect(job, event, log_ref, thresholds) -> Report`, read the result
  back and clean up, raising the errors `analyze()` would.
- `abandon(context)`, stop and remove the task instance's job after a
  failed or timed-out deferral.

The job dict is everything that crosses the deferral: `collect()` runs on
a different hook instance, rebuilt from the DAG file.

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
