# Architecture

One operator, pluggable strategy Hooks (not config-driven branching) along
two independent axes:

- Where the log comes from: a `LogSourceHook` resolves the run's event log
  to an `EventLogRef` (`log_ref.py`). Five implementations fetch the log to
  the worker and return a `LocalEventLog` (Spark History Server REST API,
  filesystem/mounted-HDFS path pattern, XCom, SFTP, and History Server
  through an SSH tunnel). Two leave it in place and return a reference:
  `RemotePathLogSourceHook` (a `RemoteEventLog`, a path on an SSH host) and
  `HistoryServerAppLogSourceHook` (a `HistoryServerApp`, which the CLI
  fetches itself through `--shs-base-url`/`--app-id`/`--attempt-id`).
- Where the analysis runs: an `AnalyzeHook` runs the
  `sparkforensics-analyze` CLI against that reference.
  `SubprocessAnalyzeHook` runs it on the worker and reads `LocalEventLog`
  and `HistoryServerApp`. `SSHAnalyzeHook` runs it on the host behind an
  `ssh_conn_id` and reads `RemoteEventLog` (same `ssh_conn_id`) and
  `HistoryServerApp`, so the log never reaches the worker and the worker
  needs no Node.js.

```
LogSourceHook.resolve(context) -> EventLogRef -> AnalyzeHook.analyze(ref, thresholds) -> Report
                                   LocalEventLog     SubprocessAnalyzeHook (worker)
                                   RemoteEventLog    SSHAnalyzeHook (SSH host)
                                   HistoryServerApp  either
```

Each backend declares the reference kinds it can read from where it runs
(`AnalyzeHook.supported_log_refs`); `AnalyzeHook.analyze()` rejects any
other kind with an error naming what it reads, before running anything, so
a mismatched pairing (say, `SSHAnalyzeHook` with a log the worker
downloaded) fails fast instead of analyzing the wrong file. Both trigger shapes (the standalone `SparkForensicsOperator` and the `spark_forensics_callback`
`on_success_callback` factory) converge on one function,
`operator.run_spark_forensics(context, ...)`, so there is exactly one
execute-analyze-persist-notify-threshold code path to maintain. Notify
always runs before the threshold check's fail/warn/ignore branching, so a
breach that raises still gets a notification out first.

## Components

- `_compat.py`, the one real Airflow-2-vs-3 difference this package
  depends on: `BaseOperator`/`BaseOperatorLink` live in `airflow.sdk` on
  3.x, `airflow.models.*` on 2.x. Same `get_link` signature both versions;
  only the import path differs.
- `log_ref.py`, the `EventLogRef` kinds (`LocalEventLog`,
  `RemoteEventLog`, `HistoryServerApp`): frozen dataclasses saying where a
  log is, the only thing a log source and a backend share.
  `log_ref_to_dict`/`log_ref_from_dict` turn one into plain JSON values
  and back, for the deferral's resume kwargs.
- `hooks/log_source/{base,history_server,filesystem,xcom}.py`, fetch the
  event log to a local path and return it as a `LocalEventLog`.
  `LogSourceHook.cleanup(log_ref)` (no-op by
  default) lets a hook remove a path it created for itself once analysis
  is done; `run_spark_forensics` calls it in a `finally`, so it runs on
  both success and failure *after* `resolve()` has returned. That `finally`
  does not cover a `resolve()` that raises before returning (there is no
  path yet to clean up via `cleanup()`), so each hook is responsible for
  cleaning up after itself in that case; currently `HistoryServerLogSourceHook`
  and `SFTPLogSourceHook` do this, via their own internal try/except
  around the download/transfer that removes their owned temp dir on any
  exception. See `docs/runbook.md`'s "Disk fills up" entry for which
  hook/config combinations actually clean up.
- `hooks/log_source/{sftp,tunnel}.py`, `SFTPLogSourceHook` and
  `SSHTunneledLogSourceHook`, for on-prem log sources reachable only over
  the SSH connection an `SSHOperator` already has configured (no cloud
  network route to the on-prem History Server / mounted path). Both
  import `SFTPHook`/`SSHHook` lazily, same reason as the existing
  provider-hook imports above. `SFTPLogSourceHook` is a standalone sibling
  of `FilesystemLogSourceHook` (differs in how it reads bytes: SFTP calls,
  not local filesystem calls). `SSHTunneledLogSourceHook` wraps any
  HTTP-based `LogSourceHook`, built by a caller-supplied factory, behind
  an SSH port forward: composition, not a History-Server-specific hook.
  The tunnel closes when `resolve()` returns, so the wrapped hook must
  fetch the log (return a `LocalEventLog`); a `HistoryServerApp` pointing
  through the tunnel is rejected.
- `hooks/log_source/{remote_path,history_server_app}.py`,
  `RemotePathLogSourceHook` and `HistoryServerAppLogSourceHook`, reference
  only: they render or copy their configuration into a `RemoteEventLog` /
  `HistoryServerApp` and do no I/O. Per-run values resolve the same way as
  in the fetching hooks: `RemotePathLogSourceHook.path_template` uses the
  shared, sanitized `_path_template` substitutions, and
  `HistoryServerAppLogSourceHook`'s `app_id`/`attempt_id` are plain
  constructor values, like `HistoryServerLogSourceHook`'s.
  `hooks/log_source/_path_template.py` and `_rolling_log.py` hold the
  path-templating and rolling-log-entry-matching logic shared between
  `filesystem.py`/`sftp.py` and `history_server.py`/`sftp.py`
  respectively. `_dest_root.py` holds the `dest_root/<basename>` staging
  convention shared by `filesystem.py`/`sftp.py`, and the owned-temp-dir
  bookkeeping (create if `dest_dir` is unset, `rmtree` on cleanup/error if
  owned) shared by `sftp.py`/`history_server.py`.
- `hooks/analyze/base.py`, `AnalyzeHook`: `analyze(log_ref, thresholds)`
  checks the reference against `supported_log_refs` (`check_log_ref()`),
  then calls the backend's `_analyze()`. `DeferrableAnalyzeHook` adds the
  detached-job interface the deferrable mode drives (`submit`,
  `trigger_for`, `defer_timeout`, `collect`, `abandon`); see "Deferrable
  execution" below.
- `hooks/analyze/_cli.py`, the CLI contract every backend shares: argument
  building for each reference kind (a positional path, or
  `--shs-base-url`/`--app-id`/`--attempt-id`), threshold flags, exit-code
  handling (0/1/3 usable, 2 and anything else raise), and building the
  `Report` from the JSON report and stderr threshold lines. Backends only
  differ in how they run the command and collect its output.
- `hooks/analyze/subprocess.py`, `SubprocessAnalyzeHook`, runs the CLI on
  the worker with `--out <tmpfile>`.
- `hooks/analyze/ssh.py`, `SSHAnalyzeHook`, runs the CLI on the SSH host
  over `SSHHook` (imported lazily, `ssh` extra) and reads the report from
  stdout, so nothing is written on that host (see "Killing a task" below
  for how it is stopped). Every argument is
  quoted with `shlex`, so rendered paths and app ids can't inject shell
  syntax.
  It reads the paramiko channel itself rather than through
  `SSHHook.exec_ssh_client_command`, which bounds only each idle read and
  logs every stdout line (the whole report) to the task log; `timeout`
  here is a wall-clock bound on the whole run, and the command runs under
  coreutils `timeout` so the remote process stops too (closing a non-PTY
  channel doesn't signal it). Exit 124 is that timeout; 126/127 from the
  remote shell means the CLI or `timeout` is missing or not executable
  there. One function, `_report_from_run`, maps an exit code, report text
  and stderr to a `Report` or an error for both the synchronous and the
  deferred path, so the two can't drift apart.
- `hooks/analyze/_remote_job.py`, the shell commands of the deferred
  path: the task-instance directory naming, the sweep that stops and
  removes an earlier try's job, the CLI command with `--out` and stderr
  redirected into the job directory, and thin wrappers over the SSH
  provider's `RemoteJobPaths`, `build_posix_wrapper_command` and
  `build_posix_cleanup_command`. Also the synchronous path's launcher,
  which reports the CLI's pid, and the command that stops it.
- `report.py`, `Report`/`ThresholdResult` dataclasses, plus parsing of the
  CLI's JSON report (`--out` file or stdout) and its stderr threshold
  lines. No
  threshold-evaluation *logic* lives here: sparkforensics's own CLI decides
  pass/violation/inconclusive, this module only parses what it printed, so
  it can never drift from what the CLI actually enforces.
- `sinks.py`, persists the report JSON (local path, `file://`, `s3://`).
- `notify.py`, the `Notifier` base class: subclasses implement `_send()`
  for whatever messaging/paging system they target, and `notify()` makes
  that best-effort, never raising. No concrete notifier ships in this
  package.
- `links.py`, `ReportLink`, a clickable "SparkForensics report" link on
  the task in the Airflow UI. On 2.x the webserver calls `get_link`, which
  reads the `return_value` XCom the operator already pushed. On 3.x the
  task runner calls `get_link` on the worker after `execute()` and stores
  the result in XCom for the API server, so `get_link` returns the
  destination `execute()` recorded on the operator's
  `persisted_report_dest` instead of reading the metadata database. That
  is empty when the run failed before `sinks.persist()`, and set on a
  threshold breach because the report is persisted before the raise.
- `plugin.py`, `SparkForensicsPlugin`, registered through the
  `airflow.plugins` entry point in `pyproject.toml`. Airflow 2.x drops any
  operator link whose class is not registered when it deserializes a DAG,
  and the webserver renders from the serialized DAG, so without it the
  link never shows. Airflow 3.x needs no registration.
- `operator.py`, `SparkForensicsOperator` + the shared
  `run_spark_forensics` core, whose second half, `handle_report()`
  (persist, warn, notify, apply `on_threshold_breach`), is also what a
  deferred run's `execute_complete()` calls.
- `callback.py`, `spark_forensics_callback(**kwargs)`, an
  `on_success_callback` factory sharing the same core.

## Deferrable execution

`SparkForensicsOperator(deferrable=True)` with `SSHAnalyzeHook` frees the
worker slot while the CLI runs on the SSH host:

```
worker:     execute()  resolve log_ref -> backend.submit() -> defer(trigger_for(job), kwargs)
triggerer:  SSHRemoteJobTrigger polls exit_code, streams stdout.log
worker:     execute_complete(event, job, log_ref, report_dest, thresholds)
              -> backend.collect()  read stderr + report.json, remove the job dir
              -> handle_report()    same persist/notify/threshold code as a sync run
```

Composition, not subclassing `SSHRemoteJobOperator`. The operator stays a
plain `BaseOperator` whose backend is pluggable; `SSHAnalyzeHook` uses the
SSH provider's building blocks (`RemoteJobPaths`, `generate_job_id`,
`build_posix_wrapper_command`, `build_posix_cleanup_command` and
`SSHRemoteJobTrigger`) behind the `DeferrableAnalyzeHook` interface.
Subclassing would have made the SSH provider an import-time dependency of
the operator, breaking the non-ssh Airflow 2.6 floor. It would also have
tied the operator to one backend and inherited an `execute_complete()`
that removes the job directory before anything reads it and fails every
non-zero exit, where exits 1 and 3 are usable reports here. No trigger of
our own is written: the provider's is the one the triggerer runs.

State across the deferral. Airflow resumes on a fresh operator rebuilt
from the DAG file, so nothing set on the instance before `defer()`
survives. Everything `execute_complete()` needs travels in the resume
kwargs, as JSON-native values that Airflow 2's `BaseSerialization` and
Airflow 3's serde both round-trip: the job dict (connection id, CLI
binary, `timeout`, every remote path), the log reference as a dict, the
rendered `report_dest` and the thresholds the job was submitted with. The
notifier and `on_threshold_breach` come from the fresh instance.

The job on the host. The CLI runs under the same coreutils `timeout` as
the synchronous path, writes its report to `--out` inside the job
directory and its stderr (the threshold lines) to a file next to it. The
provider's wrapper merges stdout and stderr into the log the trigger
streams, so the report is never parsed from that log. Each task instance
(dag, task, run, map index) gets its own directory under
`remote_base_dir`, and every `submit()` first stops whatever an earlier
try left running there and removes it. That is what keeps a retry or a
clear from running two analyses at once. Stopping a job signals its whole
session (`pkill -s`): the provider's own group kill misses the CLI,
because coreutils `timeout` moves itself into a process group of its own.
A pid is only signalled while its command line still names its job
directory, so a pid reused after a reboot is left alone.

Failure paths. A trigger error event makes `collect()` stop and remove
the job before raising. A deferral that times out (the backend's
`defer_timeout`, `timeout` plus 120 seconds, or the task's
`execution_timeout`) resumes with `next_method="__fail__"`, which never
reaches `execute_complete()`; `resume_execution()` is overridden to call
`backend.abandon(context)` first. The runbook's "Deferred runs" section
lists the behaviour for each case.

Killing a task. `SparkForensicsOperator.on_kill()` uses the context of the
`execute()` or `execute_complete()` running in the same process, never
state from before a deferral. A deferrable task abandons its task
instance's job; a synchronous one calls the backend's `on_kill()`.
Closing the SSH channel alone would leave the remote CLI running, so the
synchronous SSH command starts a small POSIX `sh` launcher in the
background, which prints `sparkforensics-pid:<its pid>` on stdout and
then execs `setsid timeout ... sparkforensics-analyze`: the pid stays the
same and becomes the session id. The hook takes that line out of stdout
as it arrives and keeps the pid. `SSHAnalyzeHook.on_kill()` closes the
channel and, over a fresh connection, signals that session, only while
the pid still runs the command line it was started with. Nothing is
written on the host, and the CLI's stdout and stderr stay on the
channel, so reports and errors are unchanged. Without `setsid` on the
host the launcher execs `timeout` directly, and the stop falls back to
`timeout`'s process group. A task killed before the pid arrives only
has its channel closed; the CLI's own `timeout` bounds it. A task
that is killed while deferred has no worker process and no `on_kill()`;
its job is stopped by the next try's sweep, or by its own `timeout`.

Why `apache-airflow-providers-ssh>=6.0.1`. Releases before it put the
job paths into the wrapper unquoted and validate cleanup only against the
default base directory; before 5.0.4 they also record the launcher's pid,
so a kill misses the command itself. 6.0.1 still splices paths into a double-quoted string inside
the job script, so `remote_base_dir` and the resolved `$HOME` are
rejected if they contain `$`, `` ` ``, `"`, `\` or control characters.

## Deferred

**http/MCP AnalyzeHook backend.** The spec originally described an "http"
backend calling a deployed sparkforensics `server/` instance. `server/`
turned out to expose only a static file server, a `/shs-proxy` CORS
passthrough, and `/mcp` (a Model Context Protocol tool server for
conversational AI clients, `resolveOrCreateRun`/`diagnoseRun`/
`evaluateBudgetsForRun`), not a `POST log -> JSON report` REST endpoint.
Building an MCP client into this package (session handling, tool-call
sequencing, stitching two tool results into one `Report`) was scoped out of
v1 as real added complexity with no upstream API sign-off. If it's ever
worth doing: `hooks/analyze/mcp.py`, an `AnalyzeHook` whose
`supported_log_refs` is `(HistoryServerApp,)` (the server resolves the run
itself, like the CLI's `--shs-base-url`), using an MCP Python client (e.g.
`mcp` on PyPI) against `POST {server_url}/mcp`, calling `resolveOrCreateRun`
then `diagnoseRun` + `evaluateBudgetsForRun`. `SSHAnalyzeHook` already
covers the main reason to want it, running the analysis off the worker next
to the logs.
