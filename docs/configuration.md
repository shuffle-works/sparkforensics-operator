# Configuration

Every class a DAG author constructs is importable from the top-level
package:

```python
from sparkforensics_operator import (
    SparkForensicsOperator, spark_forensics_callback, ThresholdBreached, Notifier,
    LogSourceHook, HistoryServerLogSourceHook, HistoryServerAppLogSourceHook,
    FilesystemLogSourceHook, XComLogSourceHook, SFTPLogSourceHook,
    SSHTunneledLogSourceHook, RemotePathLogSourceHook,
    AnalyzeHook, DeferrableAnalyzeHook, SubprocessAnalyzeHook, SSHAnalyzeHook,
    EventLogRef, LocalEventLog, RemoteEventLog, HistoryServerApp,
)
```

A few names live in submodules: `SUMMARY_XCOM_KEY` in
`sparkforensics_operator.summary`, `REMOTE_JOB_XCOM_KEY` in
`sparkforensics_operator.operator`, `log_ref_to_dict`/`log_ref_from_dict`
in `sparkforensics_operator.log_ref`, `Report`/`ThresholdResult` in
`sparkforensics_operator.report`, and `ReportLink` in
`sparkforensics_operator.links`.

## `SparkForensicsOperator`

```python
SparkForensicsOperator(
    *,
    task_id: str,
    log_source: LogSourceHook,
    backend: AnalyzeHook,
    report_dest: str,                        # local path, file://, or s3://; Jinja-templated
    max_runtime_ms: int | None = None,
    max_spill_gb: float | None = None,
    max_skew_ratio: float | None = None,
    max_failed_task_rate_pct: float | None = None,
    min_efficiency_pct: float | None = None,
    on_threshold_breach: str = "fail",       # "fail" | "warn" | "ignore"
    notifier: Notifier | None = None,
    aws_conn_id: str | None = None,          # non-default Airflow AWS connection for s3:// report_dest
    deferrable: bool | None = None,          # see "Deferrable execution"
    report_url_template: str | None = None,  # see "Report link and report URL"
    baseline_log_source: LogSourceHook | None = None,  # see "Comparing against a baseline run"
    max_regression_pct: float | None = None,
    regression_metric: str | None = None,
    fail_on_introduced: str | None = None,   # "critical" | "warning" | "info" | "all"
    **base_operator_kwargs,
)
```

All arguments are keyword-only.

- `log_source` says where the event log comes from, and `backend` where
  the analysis runs. See [log sources](#log-sources) and
  [analyze backends](#analyze-backends).
- The threshold arguments are described under
  [Thresholds](#thresholds). Only the ones you set are enforced.
- `on_threshold_breach` decides what a violated threshold does: `"fail"`
  raises `ThresholdBreached`, `"warn"` logs a warning, `"ignore"` does
  nothing. Any other value raises `ValueError` when the DAG is parsed.
- `notifier` is an optional [Notifier](#notifier).

`template_fields` is `("report_dest", "log_source", "backend",
"baseline_log_source")`: Airflow renders `report_dest` and every
[templated hook argument](#templated-hook-arguments) before `execute()`.

The operator returns the destination it persisted the report to, which
Airflow pushes to XCom as `return_value`. See
[report destinations](#report-destinations) for the exact string.

The order of work in a run is: locate the log, analyze it, persist the
report, push the summary XCom, notify, then apply `on_threshold_breach`.
A breach that raises still gets its report persisted, its summary pushed
and its notification sent first. The log source's `cleanup()` runs after
analysis on success and on failure.

### `ThresholdBreached`

A threshold breach with `on_threshold_breach="fail"` raises
`ThresholdBreached`, an `AirflowFailException`. It fails the task at once
without consuming its configured retries: the breach is a deterministic
verdict from a completed analysis, and a retry would only repeat the same
fetch and analysis to reach the same result. The message lists every
breached threshold's name and detail, and the exception's `destination`
attribute holds the persisted report's destination.

### Deferrable execution

`deferrable=True` runs the analysis as a detached job on the SSH host and
defers the task until it finishes, so no worker slot is held while the CLI
runs. It needs a backend that can run detached, a `DeferrableAnalyzeHook`.
`SSHAnalyzeHook` is the only one shipped. `deferrable=True` raises
`ValueError` when the DAG is parsed if the backend is not a
`DeferrableAnalyzeHook`, or if it reports that it cannot defer in this
environment (for `SSHAnalyzeHook`: the SSH provider is missing or older
than 6.0.1).

Left at `None`, `deferrable` follows Airflow's `[operators]
default_deferrable` setting, but only for a backend that can defer: with
`default_deferrable = True`, a `SubprocessAnalyzeHook` task still runs
synchronously instead of failing. `deferrable=False` always runs
synchronously.

A deferred run produces the same persisted report, threshold results,
report link, XCom values and notifier calls as a synchronous one, and
raises the same errors for the same outcome (exit codes, missing binary,
timeout, unparseable report, SSH failures). The backend-compatibility
check runs before the job is submitted. The deferral needs a running
triggerer; [Getting started](getting-started.md#deferred-runs) lists what
else the deployment needs, and
[Troubleshooting](troubleshooting.md#deferred-runs) how retries, timeouts
and clears behave.

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

### Comparing against a baseline run

`baseline_log_source` names an earlier run to compare this one against,
the baseline; the run under analysis is the candidate. It takes any
`LogSourceHook` and is rendered like `log_source`, so the DAG author
decides which run is the baseline: a path template on `prev_ds`, an XCom
pull, a fixed reference run. The operator keeps no state across DAG runs
and never picks a baseline by itself.

```python
check_spark_job = SparkForensicsOperator(
    task_id="check_spark_job",
    log_source=FilesystemLogSourceHook(path_template="/mnt/spark-logs/{{ ds }}/eventlog"),
    baseline_log_source=FilesystemLogSourceHook(path_template="/mnt/spark-logs/{{ prev_ds }}/eventlog"),
    backend=SubprocessAnalyzeHook(),
    report_dest="s3://reports/{{ run_id }}/report.json",
    max_regression_pct=20,                     # wall-clock, unless regression_metric says otherwise
    fail_on_introduced="critical",
)
```

The backend passes the baseline to `sparkforensics-analyze --baseline`,
which reads an event log file or rolling-log directory and nothing else.
So the baseline must resolve to a path readable where the analysis runs:
a `LocalEventLog` for `SubprocessAnalyzeHook`, a `RemoteEventLog` on the
backend's own `ssh_conn_id` for `SSHAnalyzeHook`. Any other reference,
including a `HistoryServerApp`, fails with `AirflowException` naming the
problem before the CLI runs. To compare against a run on a History
Server, fetch its log with `HistoryServerLogSourceHook` instead. The log
under analysis has no such limit: a `HistoryServerApp` candidate with a
path baseline works. A baseline that is the log under analysis, or whose
path contains or sits inside it (on the same host), also fails with
`AirflowException` before the CLI runs, since the run would be compared
against itself. That is what happens when two `FilesystemLogSourceHook`s
or `SFTPLogSourceHook`s share one `dest_dir` for same-named logs, so give
each its own. `deferrable=True` works too; the resolved baseline crosses
the deferral with the rest of the resume kwargs.

`max_regression_pct`, `regression_metric` and `fail_on_introduced` (see
[Thresholds](#thresholds)) budget the comparison. Without
`baseline_log_source`, any of them raises `ValueError` when the DAG is
parsed. A comparison runs even with none of them set: its section lands
in the persisted report under `comparison`, and its outcome in the
[summary XCom](#summary-xcom).

### Killing a task

`SparkForensicsOperator.on_kill()` stops the analysis when the task is
killed while a worker runs it. A deferrable task's backend abandons the
remote job it submitted (`DeferrableAnalyzeHook.abandon()`); a synchronous
one calls `AnalyzeHook.on_kill()`, which is a no-op by default.
`SSHAnalyzeHook.on_kill()` closes the SSH channel and stops the remote CLI
by the pid it reported, if it had reported one yet.

`on_kill()` also works on an operator whose `execute()` never ran, because
Airflow 2 calls it that way when `execution_timeout` expires during a
deferral. For that, a deferrable run keeps its submitted job in XCom under
`sparkforensics_remote_job` (`REMOTE_JOB_XCOM_KEY`) until the next try
clears it. Airflow runs no worker process for a task while it is
deferred, so killing it then reaches its job only through the next try's
sweep.

## `spark_forensics_callback`

```python
spark_forensics_callback(**kwargs) -> Callable[[dict], None]
```

Takes the same keyword arguments as `SparkForensicsOperator` except
`task_id` and the `BaseOperator` kwargs, `baseline_log_source` and the
comparison thresholds included, and returns a callable to attach
as `on_success_callback` on the upstream Spark task:

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

A callback cannot defer. `deferrable` defaults to `False` here, ignores
`[operators] default_deferrable`, and `deferrable=True` raises
`ValueError`.

Airflow does not render callback arguments, so the callback renders them
itself when it runs: `report_dest` and the templated arguments of
`log_source`, `baseline_log_source` and `backend`, with the upstream task's Jinja environment
(its DAG's macros and filters) and the callback's context. Every value is
rendered as a template string, never loaded as a template file, so a
`report_dest` ending in `.json` works on an upstream task whose
`template_ext` includes `.json` (EMR, Spark on Kubernetes). `{{ run_id }}`
in `report_dest` works as it does on the operator. The hooks are rendered
as copies, so the instances passed to the factory stay templates for the
next run. `report_url_template` is not rendered; it is filled from the
destination only.

Airflow logs and swallows any exception raised in a callback. A breach
never fails the upstream task and never retries it. Use the standalone
operator (`spark_task >> SparkForensicsOperator(...)`) if a breach must
fail the DAG.

No operator pushes this callback's return value, so the callback pushes
the persisted report's destination to XCom itself, under `return_value`
on the upstream task it is attached to, along with the
`sparkforensics_summary` XCom. Read them with
`ti.xcom_pull(task_ids="run_spark_job", key=...)`. No report link is
attached to that task.

## Log sources

A log source's `locate(context)` returns an event log reference saying
where the log is, and the backend's `analyze(log_ref, thresholds)` reads it
from there. The three reference kinds are frozen dataclasses, together
typed as `EventLogRef`:

- `LocalEventLog(path: Path)`, a file or rolling-log directory on the
  Airflow worker. A Spark event log is a single file, or, for a
  long-running application, a directory of rolling segments named
  `events_<n>_...`.
- `RemoteEventLog(ssh_conn_id: str, path: str)`, a file or rolling-log
  directory on the host behind `ssh_conn_id`.
- `HistoryServerApp(base_url: str, app_id: str, attempt_id: str | None = None)`,
  a Spark History Server application the CLI fetches itself. `base_url`
  must be reachable from wherever the analysis runs.

### Which log source works with which backend

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
...; it reads: ...") before the CLI runs. A `RemotePathLogSourceHook` and
`SSHAnalyzeHook` with different `ssh_conn_id`s raise "... cannot analyze
an event log on a different SSH host ..." instead.

### Log source hooks

- `HistoryServerLogSourceHook(base_url, app_id, attempt_id=None, dest_dir=None, timeout=300)`
  downloads the log from the History Server's
  `/api/v1/applications/{app_id}/logs` endpoint. It has no auth or session
  support: a Kerberos- or SPNEGO-protected History Server isn't reachable
  (subclass `LogSourceHook` if you need that). `timeout` bounds each
  connect and read, and also the whole transfer once the response headers
  arrive, checked after each chunk, so a slowly trickling connection
  still fails.
- `FilesystemLogSourceHook(path_template, dest_dir=None)` reads a local or
  mounted path, such as `/spark-events/{{ run_id }}`. With `dest_dir` set
  it copies the log there first.
- `XComLogSourceHook(task_id, xcom_key="return_value")` pulls the log path
  from an upstream task's XCom.
- `SFTPLogSourceHook(ssh_conn_id, path_template, dest_dir=None)` is the
  SSH-reachable equivalent of `FilesystemLogSourceHook`, for an on-prem
  path the worker can't mount. Every fetch is a remote-to-local copy.
  Requires the `ssh` extra.
- `SSHTunneledLogSourceHook(ssh_conn_id, remote_host, remote_port, hook_factory)`
  tunnels network access to any HTTP-based log source (such as
  `HistoryServerLogSourceHook`) through the SSH connection an
  `SSHOperator` already uses. `hook_factory` receives the tunnel's local
  base URL (`http://127.0.0.1:<port>`) and must return a constructed log
  source, for example
  `lambda base_url: HistoryServerLogSourceHook(base_url=base_url, app_id=...)`.
  The tunnel closes when `locate()` returns, so the wrapped hook must fetch
  the log (return a `LocalEventLog`); any other reference kind is
  rejected. Requires the `ssh` extra. For a History Server reachable only
  from the SSH host, `HistoryServerAppLogSourceHook` with `SSHAnalyzeHook`
  avoids downloading the log at all.
- `RemotePathLogSourceHook(ssh_conn_id, path_template)` points at an event
  log that stays on the host behind `ssh_conn_id`, for `SSHAnalyzeHook`
  with the same `ssh_conn_id`. It does no I/O, so a missing path surfaces
  as the CLI's exit-2 error.
- `HistoryServerAppLogSourceHook(base_url, app_id, attempt_id=None)` passes
  a History Server application to the CLI as
  `--shs-base-url`/`--app-id`/`--attempt-id` instead of downloading it. It
  has the same no-auth limitation as `HistoryServerLogSourceHook`.

The fetching hooks remove what they downloaded after analysis, but only a
path they created; [Troubleshooting](troubleshooting.md#disk-fills-up-on-a-worker)
lists which configurations are cleaned up.

### Custom log sources

Subclass `LogSourceHook` and implement `locate(context) -> EventLogRef`.
Optionally override `cleanup(log_ref)`, a no-op by default, to remove
anything `locate()` created; it receives the same reference after analysis.
In a deferred run, `cleanup()` is called on the fresh operator instance
that resumes the task, so it can't rely on state `locate()` left on the
hook. Don't define a `resolve` method: Airflow 3's templater calls
`resolve()` on any template field that has one.

## Analyze backends

- `SubprocessAnalyzeHook(analyze_bin="sparkforensics-analyze", timeout=900)`
  runs the CLI on the Airflow worker. Reads `LocalEventLog` and
  `HistoryServerApp`, and a `LocalEventLog` baseline. `timeout` is in
  seconds.
- `SSHAnalyzeHook(ssh_conn_id, analyze_bin="sparkforensics-analyze", timeout=900, remote_base_dir=None, poll_interval=5)`
  runs the CLI on the host behind `ssh_conn_id` (the same Airflow SSH
  connection `SSHOperator` uses) and reads the JSON report from its
  stdout. Reads `RemoteEventLog` on the same `ssh_conn_id` and
  `HistoryServerApp`, and a `RemoteEventLog` baseline on the same
  `ssh_conn_id`. `timeout` bounds the whole remote run in seconds,
  on the worker and on the SSH host through coreutils `timeout`. Every
  argument is shell-quoted. Requires the `ssh` extra on the worker, and
  coreutils `timeout`, Node.js 18+ and `sparkforensics-cli` on the SSH
  host; the worker needs none of them.

  With `deferrable=True` on the operator, the CLI runs as a detached job
  instead, writing its report to a file in a job directory under
  `remote_base_dir`, and the triggerer checks on it every `poll_interval`
  seconds. The deferred mode needs `apache-airflow-providers-ssh>=6.0.1`
  on the workers and the triggerer, and bash on the SSH host. Both modes
  stop a run with `setsid`, and `pkill` or else `/proc`, on the SSH host.
  A synchronous run writes nothing on the SSH host.
- `SSHAnalyzeHook.cannot_defer_reason() -> str | None` says why the
  installed SSH provider cannot run the deferrable mode, or returns `None`.

Both backends build the same CLI arguments, parse the same report and
threshold output, and treat exit codes the same way: 0, 1 and 3 produce a
report; 2 and any other code raise `AirflowException` with the CLI's
stderr. A missing binary and a timeout raise their own errors, and so do
exit 124 (the SSH host's `timeout`) and 126/127 (binary missing or not
executable on the SSH host). See [Troubleshooting](troubleshooting.md#error-messages).

### Job directories

A deferred run's job files live under `remote_base_dir`, which defaults
to `$HOME/.sparkforensics/jobs` of the SSH user. Set it to an absolute
path without `$`, `` ` ``, `"`, `\`, `..` or control characters, or the
hook raises `ValueError`; a value holding Jinja (`{{ ... }}`) is checked
once rendered.

Each task instance gets one owner-only directory there, and each try a
job directory inside it holding the report, stderr, the log the triggerer
streams and the exit code. Both are removed once the report is read, on
success and on failure. The task instance directory's name is a digest of
the task instance and the Airflow deployment's `base_url` (`[api]` on
Airflow 3, `[webserver]` on Airflow 2), so two deployments that run the
same DAGs as the same SSH user keep their jobs apart. Set a distinct
`base_url` on the workers of each (Airflow 2 defaults it to
`http://localhost:8080`, Airflow 3 leaves it unset), or give each
deployment its own `remote_base_dir`. The submitted job is also kept in the task instance's
XCom under `sparkforensics_remote_job`, so a deferral that fails, times
out or is killed stops that job even if the hook's `ssh_conn_id` or
`remote_base_dir` renders differently by then.

### Custom backends

Subclass `AnalyzeHook`, set `supported_log_refs` to the reference kinds it
can read from where it runs, and implement `_analyze(log_ref, thresholds)
-> Report`. `analyze()` calls `check_log_ref(log_ref)` first, which raises
for an unsupported kind; override it to add checks, as `SSHAnalyzeHook`
does for the SSH host. Override `on_kill()` to stop a running analysis
when the task is killed.

A backend declares the reference kinds it can pass as `--baseline` in
`supported_baseline_refs` (empty by default: no comparison). Such a
backend's `_analyze()`, and `submit()` for a deferrable one, also take a
`baseline_ref` keyword, passed only when there is a baseline and already
checked by `check_baseline_ref()`, so a custom backend written without
it keeps working for single-run analysis. `baseline_log_source` with a
backend whose `supported_baseline_refs` is empty raises `ValueError` when
the DAG is parsed.

A backend that can run detached subclasses `DeferrableAnalyzeHook` and
implements, on top of `_analyze()`:

- `submit(log_ref, thresholds, context) -> dict`: start the job and return
  it as JSON-native values. First stop and remove anything an earlier try
  of the same task instance left behind.
- `trigger_for(job)`: the trigger to defer on.
- `defer_timeout(job) -> timedelta`: how long to wait before giving up.
- `collect(job, event, log_ref, thresholds) -> Report`: read the result
  back and clean up, raising the errors `analyze()` would.
- `abandon(context, job=None)`: stop and remove the task instance's job
  after a failed or timed-out deferral or a kill. `job` is the dict
  `submit()` returned, when the operator still has it (from XCom or the
  resume kwargs); act on it rather than on the hook's current
  configuration, which may render differently by then.
- `cannot_defer_reason() -> str | None`, optional: why the backend can't
  run detached in this environment. `deferrable=True` then raises
  `ValueError`, and `[operators] default_deferrable` leaves the task
  synchronous.

The job dict is everything that crosses the deferral: `collect()` runs on
a different hook instance, rebuilt from the DAG file.
`log_ref_to_dict(log_ref)` and `log_ref_from_dict(data)` convert a
`RemoteEventLog` or `HistoryServerApp` to and from plain JSON values; the
operator uses them to carry the reference across the deferral.

## Templated hook arguments

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

Other arguments, such as `timeout`, `poll_interval`, `remote_port` and
`hook_factory`, are not templated. Each task renders its own copy of the
hooks, so one hook instance can be shared by several tasks or mapped task
instances. Rendered values are checked when used:

- a `path_template` must be fully rendered (no `{{` or `{%` left),
  non-empty and free of `..` segments, so a crafted `run_id` (say, from
  `airflow dags trigger --run-id`) or XCom value can't escape the intended
  directory;
- the old `{run_id}`-style placeholders are rejected, with an error that
  names the Jinja form;
- an `app_id` that rendered to `""` or `"None"` (as a missing XCom does)
  raises, and an `attempt_id` that did is treated as unset;
- a templated `remote_base_dir` is checked once rendered.

For `SSHTunneledLogSourceHook`, the hook `hook_factory` returns is rendered
too, at `locate()` time, with the task's context. A custom hook subclass
adds its own attribute names to `template_fields`.

## Thresholds

Every threshold argument is optional; only the ones you set are enforced.
The names mirror the `sparkforensics-analyze` CLI flags, and the CLI does
the evaluation (upstream calls a threshold a "budget"):

| argument | CLI flag | unit |
|---|---|---|
| `max_runtime_ms` | `--max-runtime` | milliseconds |
| `max_spill_gb` | `--max-spill` | gigabytes |
| `max_skew_ratio` | `--max-skew` | P95/median ratio |
| `max_failed_task_rate_pct` | `--max-failed-task-rate` | percent |
| `min_efficiency_pct` | `--min-efficiency` | percent |
| `max_regression_pct` | `--max-regression-pct` | percent the checked metric may regress past the baseline |
| `regression_metric` | `--regression-metric` | the metric `max_regression_pct` checks (default `wallClock`) |
| `fail_on_introduced` | `--fail-on-introduced` | fail on a finding the baseline did not have, of this impact band (`critical`, `warning`, `info`) or `all` |

The last three are comparison thresholds, budgets on the difference
between the run and its baseline (upstream names them `max-regression`
and `fail-on-introduced`). They need `baseline_log_source`, and
`regression_metric` needs `max_regression_pct`, as the CLI requires; any
other combination, or an unknown `fail_on_introduced` band, raises
`ValueError` when the DAG is parsed. The CLI validates
`regression_metric` itself (`wallClock`, `shuffleSpill`, `taskSkew`,
`failedTaskRate`, `diskSpill`, `gcTime`, `executorRunTime`, and the
volume metrics `inputBytes`, `outputBytes`, `taskCount`,
`executorsAdded`, which it reports as inconclusive since they have no
regression direction). A breach raises `ThresholdBreached` like any
other threshold.

Each configured threshold comes back as a pass, a violation or
inconclusive. Inconclusive means the event log lacked the evidence the
threshold needs; it is logged as a warning whatever
`on_threshold_breach` is, and never counts as a breach. A violation is a
breach, handled by `on_threshold_breach`.

## Notifier

The package ships no concrete notifier: channel integrations (Slack,
MS Teams, email, PagerDuty, ZenDuty) are out of scope for a Spark log
analysis package. Implement `Notifier` for the channel you use:

```python
from sparkforensics_operator import Notifier

class MyNotifier(Notifier):
    def _send(self, report, report_destination: str) -> None:
        ...  # deliver report/report_destination to your channel
```

The operator calls `notifier.notify(report, report_destination)` on every
run that produced a report, before applying `on_threshold_breach`.
`notify()` wraps your `_send()`, so a delivery failure is logged as a
warning and never fails the task, whatever `_send()` raises.

`report` is a `sparkforensics_operator.report.Report`, with the CLI's
`summary`, `findings`, `recommendations` and `threshold_results` (each a
`ThresholdResult` with `name`, `status` and `detail`), its `exit_code`,
and `violated`/`inconclusive` properties. The static helper
`Notifier._default_message(report, report_destination)` builds a one-line
plain-text summary ("SparkForensics report: N critical, M warning, K info
findings. Report: {report_destination}") you can use as a message body.

## Report destinations

`report_dest` is where the report JSON is written:

- a plain local path, or a mounted filesystem such as HDFS through an NFS
  gateway or FUSE mount;
- a `file://` URI without a host (`file:///absolute/path`);
- an `s3://bucket/key` URI, which needs the `s3` extra and uses the
  `aws_default` connection unless `aws_conn_id` is set.

Any other scheme raises `ValueError`. A local write is atomic: the report
goes to a temporary file in the same directory, then replaces the
destination.

The destination string the operator returns, pushes to XCom and records
in the summary is `report_dest` as rendered for `s3://` and plain paths,
and the bare local path for a `file://` URI.

## Summary XCom

Every run with a task instance also pushes a summary under the XCom key
`sparkforensics_summary` (`SUMMARY_XCOM_KEY`), before notifying and before
a threshold breach raises. Downstream tasks can branch on it without
reading the report. It is a dict:

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
| `comparison` | only when a baseline ran: see below |

`comparison` describes the run against its baseline on one metric,
`regression_metric` (`wallClock` when unset):

| key | value |
|---|---|
| `verdict` | the CLI's direction for the metric: `regression`, `improvement`, `unchanged`, `neutral` (a volume metric such as `inputBytes`, where more is neither better nor worse), or `unavailable` |
| `confidence` | `ok`, or `low` when the run names differ or few stages matched, so the deltas may compare different work |
| `reason` | why confidence is low, or `None` |
| `metric` | the metric's key |
| `baseline`, `candidate` | the metric's value in each run, or `None` |
| `regression_pct` | the metric's change relative to the baseline in percent, positive when it grew; `None` when either value is missing or the baseline is 0 |

Whether a regression breached a budget is in `breached_thresholds`
(`max-regression`, `fail-on-introduced`), as for any threshold.

A deferred run pushes the same summary. `return_value` stays the
destination string.

## Report link and report URL

The operator adds a "SparkForensics report" link to the task in the
Airflow UI (`ReportLink`). It points at the raw destination, or at a
browser URL if `report_url_template` is set.

`report_url_template` is a `str.format` template with four placeholders,
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

A template that names another placeholder, or doesn't build an `http://`
or `https://` URL, raises `ValueError` when the DAG is parsed. Nothing is
presigned: the link opens with the viewer's own access.

On Airflow 2 the webserver reads the link target from XCom (the summary,
else `return_value`). On Airflow 3 the worker computes it after the task
runs, from the operator's `persisted_report_url` or
`persisted_report_dest`. Either way the link is empty when the run failed
before persisting a report. On Airflow 2 the link only appears if the
package is installed as a provider on the processes that serialize and
render the DAG; see
[Troubleshooting](troubleshooting.md#report-link).
