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
LogSourceHook.locate(context) -> EventLogRef -> AnalyzeHook.analyze(ref, thresholds) -> Report
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

- `_compat.py`, the single place this package imports Airflow's public
  API from. `BaseOperator`/`BaseOperatorLink` live in `airflow.sdk` on
  3.x and `airflow.models.*` on 2.x. The Task SDK also re-homed `BaseHook`
  (3.1), and the exceptions, `TaskDeferred` and `conf` (3.2); their old
  paths still work there, as the same objects, behind deprecation shims.
  `_compat.py` tries the SDK path first and falls back to the old one, and
  `tests/test_compat.py` imports every module of the package in a fresh
  interpreter and fails on any deprecation warning they emit. The repo has
  no ruff configuration, so there are no ruff `AIR3` rules to enable; that
  test covers the same ground.
- `hooks/_templating.py`, `TemplatedHookMixin`, the base of every hook:
  an empty `template_fields` and a `__repr__` that shows them. See
  "Templated hook arguments" below.
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
  both success and failure *after* `locate()` has returned. That `finally`
  does not cover a `locate()` that raises before returning (there is no
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
  The tunnel closes when `locate()` returns, so the wrapped hook must
  fetch the log (return a `LocalEventLog`); a `HistoryServerApp` pointing
  through the tunnel is rejected.
- `hooks/log_source/{remote_path,history_server_app}.py`,
  `RemotePathLogSourceHook` and `HistoryServerAppLogSourceHook`, reference
  only: they render or copy their configuration into a `RemoteEventLog` /
  `HistoryServerApp` and do no I/O. Per-run values come from Jinja, the
  same way as in the fetching hooks (see "Templated hook arguments").
  `hooks/log_source/_path_template.py` checks a rendered path (rendered,
  non-empty, no `..` segment) and `_history_server_args.py` checks a
  rendered app id and attempt id; `_rolling_log.py` holds the
  rolling-log-entry-matching logic shared between `history_server.py`
  and `sftp.py`. `_dest_root.py` holds the `dest_root/<basename>` staging
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
- `summary.py`, the summary XCom (`SUMMARY_XCOM_KEY`,
  `build_summary()`) and the report URL (`render_report_url()`,
  `validate_report_url_template()`). See "Summary XCom and report URL"
  below.
- `links.py`, `ReportLink`, a clickable "SparkForensics report" link on
  the task in the Airflow UI. It links to the report URL when
  `report_url_template` is set, and to the raw destination otherwise. On
  2.x the webserver calls `get_link`, which reads the summary XCom (and
  falls back to the `return_value` XCom for runs that pushed no summary).
  On 3.x the task runner calls `get_link` on the worker after `execute()`
  and stores the result in XCom for the API server, so `get_link` returns
  what `execute()` recorded on the operator (`persisted_report_url`, else
  `persisted_report_dest`) instead of reading the metadata database. That
  is empty when the run failed before `sinks.persist()`, and set on a
  threshold breach because the report is persisted before the raise.
- `get_provider_info.py`, the Airflow provider metadata, returned through
  the `apache_airflow_provider` entry point in `pyproject.toml`: package
  name, version, the operator and hook modules, and `ReportLink` under
  `extra-links`. Airflow lists the package under `airflow providers list`.
  Airflow 2.x drops any operator link whose class no provider or plugin
  registered when it deserializes a DAG, and the webserver renders from
  the serialized DAG, so this registration is what makes the link show
  there. Airflow 3.x needs no link
  registration. The module imports nothing from the rest of the package,
  because the ProvidersManager can load it while Airflow itself is still
  importing.
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
worker:     execute()  locate log_ref -> backend.submit() -> defer(trigger_for(job), kwargs)
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
job dict and the log reference are built from the rendered hooks, so
templated hook arguments cross the deferral as the values the job ran
with. The notifier, `on_threshold_breach` and `report_url_template` come
from the fresh instance. The job dict is also pushed to XCom
(`sparkforensics_remote_job`) right after submitting, because Airflow
drops the resume kwargs on the failure paths below: a deferral that
fails or times out resumes with only an error, and `on_kill()` gets no
arguments at all. Airflow keeps a task instance's XComs across a
deferral and clears them at its next try.

The job on the host. The CLI runs under the same coreutils `timeout` as
the synchronous path, writes its report to `--out` inside the job
directory and its stderr (the threshold lines) to a file next to it. The
provider's wrapper merges stdout and stderr into the log the trigger
streams, so the report is never parsed from that log. Each task instance
(dag, task, run, map index) of each Airflow deployment gets its own
directory under `remote_base_dir`, and every `submit()` first stops
whatever an earlier try left running there and removes it. That is what
keeps a retry or a clear from running two analyses at once. The
deployment is part of the directory's name because two deployments
running the same DAGs share dag, task and run ids (scheduled runs are
named after their date), and with one SSH user each deployment's sweep
would stop the other's jobs. Nothing a worker sees identifies a
deployment and survives both a retry and a clear, bar configuration, so
the key is `base_url` (`[api]` on Airflow 3, `[webserver]` on Airflow 2),
which differs between real deployments; `remote_base_dir` separates two
that share it. Stopping a job signals its whole session: the provider's
own group kill misses the CLI, because coreutils `timeout` moves itself
into a process group of its own. The session is signalled with
`pkill -s`, or, on a host without procps, process by process from the
session ids in `/proc/<pid>/stat`. A pid is only signalled while its
command line still names its job directory, so a pid reused after a
reboot is left alone.

Failure paths. A trigger error event makes `collect()` stop and remove
the job before raising. A deferral that times out (the backend's
`defer_timeout`, `timeout` plus 120 seconds, or the task's
`execution_timeout`) resumes with `next_method="__fail__"`, which never
reaches `execute_complete()`; `resume_execution()` is overridden to call
`backend.abandon(context, job)` first, with the job from XCom. Airflow 2
takes a different path when `execution_timeout` ran out during the
deferral: its task runner raises `AirflowTaskTimeout` before calling
anything on the resumed operator, then calls `on_kill()`, which is why
`on_kill()` does not depend on `execute()` having run. Anything that fails
between `submit()` and `defer()` (building the trigger, the timeout, the
deferral itself) abandons the job before the error propagates, and a
task timeout or kill there reaches `on_kill()`, which does the same. The
runbook's "Deferred runs" section lists the behaviour for each case.

Killing a task. `SparkForensicsOperator.on_kill()` uses the context of the
`execute()` or `execute_complete()` running in the same process, or,
when neither ran, the context of the task running in the process
(`get_current_context()`); never state from before a deferral. A
deferrable task abandons its job: the one this process submitted or
resumed with, else the one in XCom, else whatever the task instance's
directory holds. A synchronous one calls the backend's `on_kill()`. SSH
failures are rewrapped as `AirflowException` only when they are
`Exception`s: `AirflowTaskTimeout` and the kill signal's exception reach
the task runner unchanged, since it calls `on_kill()` only for those.
Closing the SSH channel alone would leave the remote CLI running, so the
synchronous SSH command starts a small POSIX `sh` launcher in the
background, which prints `sparkforensics-pid:<its pid>` on stdout and
then execs `setsid timeout ... sparkforensics-analyze`: the pid stays the
same and becomes the session id. The hook takes that line out of stdout
as it arrives and keeps the pid. It still keeps it after an
`AirflowTaskTimeout` or kill has unwound the run, since the task runner
calls `on_kill()` only afterwards; a normal return or an ordinary
exception clears it. `SSHAnalyzeHook.on_kill()` closes the
channel and, over a fresh connection, signals that session, only while
the pid still runs the command line it was started with. Nothing is
written on the host, and the CLI's stdout and stderr stay on the
channel, so reports and errors are unchanged. Without `setsid` on the
host the launcher execs `timeout` directly, and the stop falls back to
`timeout`'s process group. A task killed before the pid arrives only
has its channel closed; the CLI's own `timeout` bounds it. A task
that is killed while deferred has no worker process and no `on_kill()`;
its job is stopped by the next try's sweep, or by its own `timeout`.

Why the deferrable mode needs `apache-airflow-providers-ssh>=6.0.1`.
Releases before it put the job paths into the wrapper unquoted and
validate cleanup only against the default base directory; before 5.0.4
they also record the launcher's pid, so a kill misses the command itself.
6.0.1 still splices paths into a double-quoted string inside the job
script, so `remote_base_dir` and the resolved `$HOME` are rejected if they
contain `$`, `` ` ``, `"`, `\` or control characters. The ssh extra
itself only asks for 3.7.1, the release Airflow 2.6's constraints pin:
the synchronous backend and the SFTP and tunnel log sources use nothing
newer, and 6.0.1 needs Airflow 2.11. `SSHAnalyzeHook.cannot_defer_reason()`
checks the installed provider, so `deferrable=True` on an older one fails
when the DAG is parsed, and `[operators] default_deferrable` leaves it
synchronous.

## Templated hook arguments

Hook arguments are Jinja templates, rendered by Airflow's own nested
template-field mechanism, the same one used for any object assigned to a
templated operator field. `SparkForensicsOperator.template_fields` is
`("report_dest", "log_source", "backend")`, and each hook class declares
the attributes it templates in its own `template_fields`; Airflow renders
those in place, on Airflow 2 and 3 alike. There is one mechanism, and no
hook formats a string itself. The earlier `{run_id}`-style `str.format`
placeholders in `path_template` are gone, and a value that still holds
one is rejected with an error naming the Jinja replacement.

Three details keep this safe.

- `render_template_fields()` gives the task its own shallow copy of
  `log_source` and `backend` before rendering, because Airflow renders in
  place and the same hook instance is often shared by several tasks or by
  every mapped instance.
- The log-source method is `locate()`, not `resolve()`. Airflow 3's
  templater calls `resolve(context)` on any template-field value that has
  a `resolve` attribute, so a hook with that method would run during
  rendering.
- Rendered values are checked where they are used, not in `__init__`
  (which sees the raw template): a path must be rendered, non-empty and
  free of `..` segments, and a History Server app id that rendered to
  `""` or `"None"` (a missing XCom) is an error. `SSHAnalyzeHook` checks
  `remote_base_dir` in `__init__` only when it holds no Jinja, and again
  before each use.

`SSHTunneledLogSourceHook` renders the inner hook its factory builds at
`locate()` time, with the task's context, since that hook does not exist
when Airflow renders the task. `spark_forensics_callback` runs outside
any template rendering, so it renders `report_dest`, `log_source` and
`backend` itself, on copies, with the upstream task's Jinja environment.
Both render through a copy of the task with an empty `template_ext`, so a
value ending in `.json` is rendered as a string, not looked up as a
template file on an EMR or Spark-on-Kubernetes upstream.

## Summary XCom and report URL

Before notifying and before any threshold breach raises,
`handle_report()` pushes a summary under the `sparkforensics_summary`
XCom key: the schema version, the destination, the report URL, the
impact-band counts, whether the run violated a threshold, the breached
and inconclusive threshold names, and the CLI exit code. Downstream tasks
can branch on it without reading the report. It is built from the same
`Report` on both the synchronous and the deferred path, and
`return_value` stays the destination.

`report_url_template` is an optional `str.format` template, such as an S3
console or an internal viewer URL, filled from the destination's
`{destination}`, `{bucket}`, `{key}` and `{path}`, each percent-encoded.
It must build an http(s) URL, checked in `__init__`. The package never
presigns anything: the URL points at a place the viewer's own
credentials open. `ReportLink` shows that URL when it is set and the raw
destination when it is not.

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
