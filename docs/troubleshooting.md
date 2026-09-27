# Troubleshooting

Entries are grouped by where the failure happens. Search this page for
the start of the error message in your task log.

## Error messages

### Running the analysis

#### "sparkforensics-analyze binary not found"

Node.js or the npm package isn't installed on this worker, or
`analyze_bin` points at the wrong path. Check with `which
sparkforensics-analyze` on the worker, and see [worker
prerequisites](getting-started.md#worker-prerequisites-subprocessanalyzehook).

#### "sparkforensics-analyze [on the SSH host (...)] failed to parse the event log, could not fetch it from the Spark History Server, or was given bad arguments (exit 2)"

One of these:

- the log isn't a valid Spark event log or rolling-log directory;
- the path doesn't exist on the host running the analysis
  (`RemotePathLogSourceHook` does not check it beforehand);
- the CLI's `--shs-base-url` fetch failed: wrong `base_url`, `app_id`
  or `attempt_id`, or the History Server is unreachable from that host;
- an unsupported CLI argument was passed.

The raised `AirflowException`'s message includes the CLI's stderr.

#### "sparkforensics-analyze exited ... but its JSON report could not be parsed"

The CLI exited 0, 1 or 3 but its report was not valid JSON. With
`SSHAnalyzeHook` the usual cause is the login shell printing a banner or
message to stdout in non-interactive sessions (an `echo` in `~/.bashrc`,
say); make it print only for interactive shells.

#### "sparkforensics-analyze exited with unexpected code {returncode}"

The CLI exited with a code other than 0, 1, 2 or 3, for example after an
OOM kill or a wrapper-script failure. No report is read for these codes;
the exception's stderr fragment shows the underlying cause.

#### "sparkforensics-analyze timed out after {timeout}s"

The CLI didn't finish within the backend's `timeout` (default 900
seconds). Raise `timeout` for large or rolling event logs, or check the
worker (or, when the message says "on the SSH host", that host) for
resource contention. `SSHAnalyzeHook` runs the CLI under coreutils
`timeout` on the SSH host, so the remote process stops after the same
`timeout` even though closing the SSH channel sends it no signal.

#### "... cannot analyze ...; it reads: ..."

The log source and backend don't fit together, for example
`SSHAnalyzeHook` with a log the worker downloaded, or
`SubprocessAnalyzeHook` with `RemotePathLogSourceHook`. See [which log
source works with which
backend](configuration.md#which-log-source-works-with-which-backend).
"cannot analyze an event log on a different SSH host" means
`RemotePathLogSourceHook` and `SSHAnalyzeHook` were given different
`ssh_conn_id`s.

#### "... cannot use ... as the baseline"

`baseline_log_source` resolved to something the backend cannot pass to
`sparkforensics-analyze --baseline`, which reads only an event log path
where the analysis runs. "A Spark History Server application cannot be a
baseline" means it is a `HistoryServerAppLogSourceHook`: fetch the
baseline with `HistoryServerLogSourceHook` (for `SubprocessAnalyzeHook`)
or point `RemotePathLogSourceHook` at its log on the SSH host (for
`SSHAnalyzeHook`). "cannot compare against a baseline on a different SSH
host" means the baseline's `ssh_conn_id` differs from the backend's. See
[comparing against a baseline
run](configuration.md#comparing-against-a-baseline-run).

#### "The baseline (...) overlaps the log under analysis"

Both log sources resolved to the same log, or one's path contains the
other's, so the run would be compared against itself. The usual cause is
two `FilesystemLogSourceHook`s or `SFTPLogSourceHook`s sharing one
`dest_dir` for logs with the same file name (`{{ ds }}/eventlog` and
`{{ prev_ds }}/eventlog`): staging the baseline overwrites the run's
copy. Give each its own `dest_dir`, or check that the two path templates
render to different runs.

#### "Unknown --regression-metric" (exit 2)

The CLI does not know that `regression_metric` key; its message lists
the valid ones.

### SSH analysis host

#### "sparkforensics-analyze binary not found or not executable on the SSH host (ssh_conn_id=...)"

The remote shell exited 126 or 127. The CLI, or the coreutils `timeout`
it runs under, isn't installed for the SSH login user, or it is but a
non-interactive session doesn't have it on `PATH`; the stderr in the
message says which. See [SSH host
prerequisites](getting-started.md#ssh-host-prerequisites-sshanalyzehook).
For the CLI, passing `analyze_bin=<full path>` is the usual fix.

#### "SSH remote analysis failed (ssh_conn_id=...) while ..."

An SSH transport failure while `SSHAnalyzeHook` connected or read a
command's output (auth failure, host unreachable, dropped connection).
The wrapped error is in the message; check `ssh_conn_id` against the
host. The message names the step: "while running sparkforensics-analyze
on ..." for a synchronous run, or "while submitting the remote analysis
job", "while preparing the remote job directory" or "while reading the
remote analysis result" for a deferred one.

#### "SSH remote analysis failed on the SSH host (ssh_conn_id=...) while ... (exit N): ..."

The connection worked but a deferred-run command exited non-zero on the
host. On submit this usually means bash is missing on the host or
`remote_base_dir` isn't writable by the login user; the host's stderr
follows the exit code.

#### `ValueError` or error saying "remote_base_dir must be an absolute path" or "cannot contain ..."

`remote_base_dir` must be an absolute path with no `..` segment. The SSH
provider's job wrapper also can't quote a path containing `$`, `` ` ``,
`"`, `\` or control characters. When the message names "the remote job
directory" rather than `remote_base_dir`, the SSH user's `$HOME`
contains one of those characters; pass a plain absolute path as
`remote_base_dir`.

### Fetching the event log

#### "Spark History Server log download failed ({status_code})"

The History Server rejected the `GET .../logs` request, for example
because the `app_id` or `attempt_id` doesn't exist, or the server is
misconfigured. Check `base_url`, `app_id` and `attempt_id`, and request
the same URL directly to see the History Server's own error.

#### "Spark History Server log download for app {app_id} exceeded {timeout}s"

The transfer took longer than `HistoryServerLogSourceHook`'s `timeout`
even though no single read or connect stalled (a slowly trickling
connection). Raise `timeout` for large event logs, or check network
throughput to the History Server.

#### "Unexpected Spark History Server log archive contents for app {app_id}"

The History Server's log zip was neither a single bare event-log file
nor a rolling log whose `events_<n>_...` entries all sit directly under
one `eventlog_v2_<appId>/` folder (the layout Spark's History Server
writes). "not under exactly one folder" means the entries are flat,
nested deeper, or split across folders (several attempts, for example:
set `attempt_id`). "has no events_<n>_ rolling-log entries" means the
one folder holds no rolling segments. This is raised in `locate()`,
before `sparkforensics-analyze` runs; compare the entry names in the
message with what the History Server returned.

#### "Configured log path does not exist"

`FilesystemLogSourceHook`'s `path_template` rendered to a path that
isn't there, because of a wrong `ds`, `run_id` or `dag_id` substitution
or because the log hasn't landed yet. Check the rendered path in the
message against the filesystem or mount.

#### "Configured log path does not exist (checked over SFTP...)"

`SFTPLogSourceHook`'s `path_template` rendered to a remote path the SFTP
server reports missing. Check the rendered path and `ssh_conn_id` in the
message against the on-prem host.

#### "Unexpected SFTP log directory contents (not a rolling-log layout)"

`SFTPLogSourceHook`'s path is a directory, but holds no `events_<n>_...`
rolling-log segments. Point `path_template` at the event log file or at
the rolling-log directory itself.

#### "SFTP log fetch failed (ssh_conn_id=...)"

An SSH or SFTP transport failure (auth failure, host unreachable,
network timeout) during `SFTPLogSourceHook.locate()`. Check
`ssh_conn_id` and the remote path in the message against the Airflow
connection and the on-prem host; the wrapped error is in the message.

#### "No XCom value found for task_id=... key=..." or "XCom-provided event log path does not exist"

`XComLogSourceHook` found no path in the upstream task's XCom, or the
path it found isn't on this worker.

#### "SSH tunnel setup failed (ssh_conn_id=..., remote=...)"

The same kind of transport failure, while opening the tunnel
`SSHTunneledLogSourceHook` needs before it can build and call its
wrapped hook. Check `ssh_conn_id` and `remote_host:remote_port` against
the on-prem host.

#### "SSH tunnel teardown failed after a successful fetch (ssh_conn_id=...)"

The wrapped hook's `locate()` succeeded, but closing the tunnel
afterwards raised. The fetched result is lost even though the log was
retrieved; check for a mid-task disconnect, then rerun the task.

#### A task using `SSHTunneledLogSourceHook` hangs instead of failing

`SSHHook.get_tunnel()` has no documented connect timeout of its own. If
the on-prem host is unreachable at the SSH layer (as opposed to the
wrapped HTTP hook's own `timeout`), opening the tunnel can hang rather
than raise, tying up a worker slot. Bound it with the task's
`execution_timeout`.

### Templated arguments

#### "path_template ... was not rendered"

The hook's `locate()` ran on a raw Jinja template.
`SparkForensicsOperator` and `spark_forensics_callback` render hook
arguments first, so this shows up when a hook is called on its own, or
when a custom wrapper hook builds another hook without rendering it
(`SSHTunneledLogSourceHook` renders the hook its factory returns).
Render it with the task's `render_template()`, or pass the rendered
value.

#### "path_template ... uses the {run_id} placeholder, which is no longer supported"

`path_template` is a Jinja template. Replace `{run_id}` with `{{ run_id
}}`, `{ds}` with `{{ ds }}`, and so on; a date format becomes `{{
logical_date.strftime('%Y-%m-%d') }}`.

#### "path_template rendered to ..., which contains a '..' segment" or "rendered to an empty path"

A value Jinja filled in (a `run_id` passed to `airflow dags trigger
--run-id`, an XCom, a param) would move the path outside its directory,
or was empty. Check where the value came from; the hook refuses to use
it.

#### "path_template resolved to a remote path/directory with no name component ..."

`SFTPLogSourceHook`'s `path_template` rendered to a path ending at the
filesystem root (a template bug that reduces to `"/"`, say), leaving
nothing to name the staged local file or directory. Compare the rendered
path in the message with `path_template` and the values Jinja filled in.

#### "app_id rendered to '' ..." or "app_id rendered to 'None' ..."

A History Server hook's `app_id` template rendered to nothing, typically
an `xcom_pull` of a key the upstream task never pushed (Jinja renders a
missing XCom as `None`). Check the upstream task's XCom and the
`task_ids` and `key` in the template. An `attempt_id` that renders to
nothing is treated as unset instead.

### Thresholds

#### `ThresholdBreached` raised

Expected when `on_threshold_breach="fail"` and a configured threshold
was violated. The message lists every breached threshold's name and
detail. The task fails without consuming its retries; see
[`ThresholdBreached`](configuration.md#thresholdbreached).

#### A threshold shows as "inconclusive" every run

The event log lacks the evidence that threshold needs (no
`ApplicationEnd` event, or no trustworthy task-level metrics, for
example). It is logged as a warning whatever `on_threshold_breach` is,
and is not a failure.

For `max-regression`, the metric could not be measured in one of the
runs, or it is a volume metric (`inputBytes`, `outputBytes`,
`taskCount`, `executorsAdded`) with no regression direction. The
summary's `comparison.verdict` says which (`unavailable` or `neutral`).

#### `ValueError: ... need baseline_log_source` or "regression_metric needs max_regression_pct" at DAG parse

The comparison thresholds follow the CLI's own rules: all three need a
baseline, and `regression_metric` only picks the metric
`max_regression_pct` checks.

#### Comparison `confidence` is `low`

The two runs have different application names or share under half their
stages, so the baseline may be a different job. Check which run
`baseline_log_source` resolves to; the regression budget is still
enforced.

## Deferred runs

A deferred run (`deferrable=True`) adds a triggerer and a detached job
on the SSH host. [Getting started](getting-started.md#deferred-runs)
lists what it needs.

### A deferred task stays `deferred` and never resumes

No triggerer is running, or it can't load the trigger. Check the
triggerer log for an import error naming
`airflow.providers.ssh.triggers.ssh_remote_job` or `asyncssh`, and
install `sparkforensics-operator[ssh]` there. The task fails on its own
once the deferral times out.

### A deferred task fails with "lost track of remote job ... while waiting for it"

The triggerer gave up reaching the SSH host. Its log shows the
connection errors ("Failed to connect to remote host", "Lost SSH
connection while polling"). A connection that works from the workers but
not from the triggerer usually needs a `key_file` that exists on the
triggerer host, a `known_hosts` or `host_key` entry there, or an extra
the async hook doesn't read.

### A deferred task fails with "Trigger timeout" or "Trigger/execution timeout"

The job didn't report back within `timeout` plus 120 seconds, or within
the task's `execution_timeout`. The task log's "SparkForensics deferred
analysis did not finish" line precedes the stop and cleanup of the job.
Check the SSH host for a reboot or an externally killed job.

### Old job directories pile up under `remote_base_dir`

From task instances marked failed or success while deferred, or whose
cleanup couldn't reach the host (logged as "Could not remove remote job
directory"). See the reaper suggestion below.

### What happens when a deferred run goes wrong


| Situation | What happens |
|---|---|
| The analysis runs past `timeout` | Coreutils `timeout` stops the CLI on the host (exit 124), the trigger fires, and the task fails with the same "timed out after {timeout}s on the SSH host" error as a synchronous run. The job directory is removed. |
| The job never reports back (host rebooted, job killed from outside) | The deferral gives up 120 seconds past `timeout`, counted from submission, or at the task's `execution_timeout` if that comes first. The task resumes only to stop the job and remove its directory, if the host is reachable. On Airflow 3 it then fails with `TaskDeferralTimeout` ("Trigger timeout"). On Airflow 2 it fails with `TaskDeferralError` ("Trigger/execution timeout"), or, when `execution_timeout` ran out first, with `AirflowTaskTimeout`, after which the task runner calls `on_kill()` to do the same cleanup. |
| The triggerer can't reach the host | It retries, tolerating five consecutive connection failures, and fires an error event on the sixth; any other error ends the trigger at once. The task stops and removes the job (best effort) and fails with "lost track of remote job ... while waiting for it". |
| A retry, or a clear while deferred | Every try first stops any job an earlier try of the same task instance left running and removes its directory, then submits its own. There is never more than one analysis per task instance on the host, and a new try never reuses an old try's output. |
| A worker restarts while the task is deferred | Nothing: no worker holds the task. A worker that dies while submitting or reading back fails the try as usual, and the next try cleans up. |
| The triggerer restarts | The trigger resumes elsewhere from its serialized state. The job keeps running on the host, detached from any SSH session. The streamed log may repeat from the start. |
| The task is killed, cleared or marked failed while a worker runs it (submitting the job or reading it back) | `on_kill()` stops the task instance's remote job and removes its directory before the worker exits. |
| The task is marked failed or success while deferred, and never runs again | No worker process holds a deferred task, so there is no `on_kill()` to run: the job runs to completion or to `timeout`, and its directory stays until that task instance's next try. A clear starts that next try, which stops the job first. |

For hosts where that last case is common, add an age-based reaper for
`remote_base_dir`, such as a daily `find ~/.sparkforensics/jobs
-mindepth 1 -maxdepth 1 -mtime +7 -exec rm -r {} +`.

## Report link

### The "SparkForensics report" link in the UI is blank

On Airflow 2 the webserver reads the link from XCom (the
`sparkforensics_summary` key, else `return_value`) and shows an empty
link if that read fails, rather than breaking the task page. Check the
webserver log for a "ReportLink could not read the report destination
from XCom" error and its traceback; the report itself was still
persisted and is in XCom under `return_value`. On Airflow 3 the worker
computes the link after the task runs, from the destination the run
persisted. The link is blank by design when the task failed before
persisting a report (the event log fetch or the analysis raised, for
example); the task log shows that error. A failure computing the link
itself shows in the task log as "Failed to push an xcom for task
operator extra link".

### The link is missing from the task page entirely (Airflow 2)

The webserver renders from the serialized DAG, and Airflow drops
`ReportLink` during deserialization unless a provider registered it.
This package registers it through its `apache_airflow_provider` entry
point. When that registration is missing, the DAG processor or scheduler
log shows "Operator Link class
'sparkforensics_operator.links.ReportLink' not registered". Install
`sparkforensics-operator` as a package (not only copied into the DAGs
folder) on every process that serializes or renders the DAG, not only on
workers, and check that `airflow providers list` lists it.

### The link opens the raw destination instead of a browser page

`report_url_template` is not set, or the run was persisted before it
was. Set it to a URL your viewers can open, such as an S3 console URL
built from `{bucket}` and `{key}`; see [report link and report
URL](configuration.md#report-link-and-report-url). The link needs the
viewer's own access to that location; nothing is presigned.

## Disk fills up on a worker

A single event log can be hundreds of MB to several GB, so repeated runs
matter on a busy worker. Each log source cleans up after itself, but
only a path it created:

- `HistoryServerLogSourceHook` with no `dest_dir` downloads to a private
  temp directory and removes it after analysis, including when `locate()`
  itself fails partway through.
- `HistoryServerLogSourceHook` with `dest_dir` set writes into a
  caller-managed shared directory the hook doesn't own, so it is **not**
  cleaned up. Point it at a scratch volume with its own external cleanup
  (`tmpwatch` or a cron job, for example).
- `FilesystemLogSourceHook` with `dest_dir` set copies the log there and
  removes that copy after analysis.
- `FilesystemLogSourceHook` with no `dest_dir` returns the caller's own
  event log path unmodified; this package never deletes it.
- `SFTPLogSourceHook` with no `dest_dir` downloads to a private temp
  directory and removes it after analysis, including when `locate()`
  itself fails partway through.
- `SFTPLogSourceHook` with `dest_dir` set writes into a caller-managed
  shared directory, so it is **not** cleaned up. The same external-cleanup
  advice applies.
- `SSHTunneledLogSourceHook` has no cleanup of its own: it delegates
  `cleanup()` to the hook `hook_factory` built, so that hook's entry
  above applies.
- `XComLogSourceHook` reads a path the upstream task wrote and never
  deletes it.
- `RemotePathLogSourceHook` and `HistoryServerAppLogSourceHook` write
  nothing to the worker. `SSHAnalyzeHook` reads the report from stdout
  and, run synchronously, writes nothing on the SSH host. Deferred, it
  writes a job directory under `remote_base_dir` that holds the report,
  and removes it once the report is read (see [deferred runs](#deferred-runs)).
