# Runbook

## Prerequisites on the worker

- With `SubprocessAnalyzeHook`, Node.js 18 or newer (the
  `sparkforensics-cli` package declares `node >=18`) and the
  `sparkforensics-cli` npm package installed
  (`npm install -g sparkforensics-cli`), with `sparkforensics-analyze`
  resolvable on `PATH`, or pass `SubprocessAnalyzeHook(analyze_bin=<full path>)`.
  With `SSHAnalyzeHook` the worker needs neither: see the next section.
- `pip install sparkforensics-operator[s3]` if `report_dest` is `s3://...`.
  Uses the `aws_default` Airflow connection unless `aws_conn_id` is set.
- `pip install sparkforensics-operator[ssh]` if using `SFTPLogSourceHook`,
  `SSHTunneledLogSourceHook` or `SSHAnalyzeHook`. All reuse the `ssh_conn_id` Airflow
  Connection already configured for `SSHOperator`: no new connection
  type to set up. Note: this extra's floor
  (`apache-airflow-providers-ssh>=5.0`) transitively requires
  `apache-airflow>=2.11`, higher than this package's own overall
  `apache-airflow>=2.6` floor.
- Any dependency your own `Notifier` implementation needs (e.g. a
  provider package for Slack/Teams/PagerDuty, an SMTP library) is on you
  to install; this package declares none for notification.

## Prerequisites on the SSH host (remote analysis)

`SSHAnalyzeHook` runs `sparkforensics-analyze` on the host behind its
`ssh_conn_id`, typically the node running the Spark History Server or one
that mounts the event log directory. The log is read there and never
crosses the network; only the JSON report comes back. That also means the
Airflow workers, including managed ones such as Amazon MWAA where
installing Node.js is awkward, don't need Node.js or the CLI at all: they
only need this package with the `ssh` extra and network access to the SSH
host.

On the SSH host:

- Install Node.js 18 or newer and `npm install -g sparkforensics-cli` for
  the user `ssh_conn_id` logs in as.
- Check the binary resolves in a non-interactive session, the kind
  `SSHAnalyzeHook` opens: `ssh <user>@<host> 'command -v sparkforensics-analyze'`.
  A non-interactive session often skips the profile that puts `nvm` or
  a custom npm prefix on `PATH`; if the command prints nothing, pass the
  full path from an interactive `command -v sparkforensics-analyze` as
  `SSHAnalyzeHook(analyze_bin=...)`.
- Coreutils `timeout` must be on that `PATH` too (it is on any standard
  Linux host), since `SSHAnalyzeHook` wraps the CLI in it.
- For `HistoryServerAppLogSourceHook`, `base_url` is resolved on this
  host, so `http://localhost:18080` reaches a History Server running on
  it. Check it with `ssh <user>@<host> 'curl -s http://localhost:18080/api/v1/applications?limit=1'`.
- For `RemotePathLogSourceHook`, the login user needs read access to the
  rendered path.

## Releasing to PyPI

`.github/workflows/publish.yml` builds and publishes on every GitHub
Release (`release: published`), using PyPI trusted publishing (OIDC), no
API token stored in the repo.

One-time setup before the first release:
- On PyPI, add a trusted publisher for this project: owner
  `shuffle-works`, repo `sparkforensics-operator`, workflow `publish.yml`,
  environment `pypi`.
- In the GitHub repo settings, create an environment named `pypi`
  (optionally with required reviewers) to match.

To cut a release: bump `version` in `pyproject.toml`, tag it, and publish
a GitHub Release from that tag. The workflow builds the sdist/wheel and
publishes them.

## Troubleshooting

- **Task fails with "sparkforensics-analyze binary not found"**, Node.js
  or the npm package isn't installed on this worker, or `analyze_bin`
  points at the wrong path. Verify with
  `which sparkforensics-analyze` on the worker.
- **Task fails with "sparkforensics-analyze [on the SSH host (...)] failed
  to parse the event log, could not fetch it from the Spark History Server,
  or was given bad arguments (exit 2)"**, the log isn't a valid Spark
  event log/rolling-log directory, the path doesn't exist on the host
  running the analysis (`RemotePathLogSourceHook` does not check it
  beforehand), the CLI's `--shs-base-url` fetch failed (wrong `base_url`,
  `app_id` or `attempt_id`, or the History Server is unreachable from that
  host), or an unsupported CLI argument was passed. Check the raised
  `AirflowException`'s message, which includes the CLI's stderr.
- **Task fails with "sparkforensics-analyze binary not found or not
  executable on the SSH host (ssh_conn_id=...)"**, the remote shell exited
  126 or 127. The CLI, or the coreutils `timeout` it runs under, isn't
  installed for the SSH login user, or it is but a non-interactive session
  doesn't have it on `PATH`; the stderr in the message names which one.
  See "Prerequisites on the SSH host" above; for the CLI, passing
  `analyze_bin=<full path>` is the usual fix.
- **Task fails with "SSH remote analysis failed (ssh_conn_id=...,
  command=...)"**, an SSH transport failure while `SSHAnalyzeHook` connected
  or read the command's output (auth failure, host unreachable, dropped
  connection). The wrapped error is in the message; check `ssh_conn_id`
  against the host.
- **Task fails with "... cannot analyze ...; it reads: ..."**, the log
  source and backend don't fit together, e.g. `SSHAnalyzeHook` with a log
  the worker downloaded, or `SubprocessAnalyzeHook` with
  `RemotePathLogSourceHook`. See the log source/backend table in
  `docs/api-reference.md`. "cannot analyze an event log on a different SSH
  host" means `RemotePathLogSourceHook` and `SSHAnalyzeHook` were given
  different `ssh_conn_id`s.
- **Task fails with "sparkforensics-analyze exited ... but its JSON report
  could not be parsed"**, the CLI exited 0, 1 or 3 but its report was not
  valid JSON. With `SSHAnalyzeHook` the usual cause is the login shell
  printing a banner or message to stdout on non-interactive sessions (e.g.
  an `echo` in `~/.bashrc`); make it print only for interactive shells.
- **Task fails with "sparkforensics-analyze exited with unexpected code
  {returncode}"**, the CLI exited with a code other than 0, 1, 2 or 3 (e.g.
  an OOM kill or a wrapper-script failure). The `--out` file is not trusted
  for these codes; check the exception message's stderr fragment for the
  underlying cause.
- **Task fails with "sparkforensics-analyze timed out after {timeout}s"**,
  the CLI didn't finish within the backend's configured `timeout` (default
  900s). Raise `timeout` for large or rolling event logs, or check the
  worker (or, with "on the SSH host", that host) for resource contention.
  `SSHAnalyzeHook` runs the CLI under coreutils `timeout` on the SSH host,
  so the remote process is stopped after the same `timeout` even though
  closing the SSH channel sends it no signal.
- **Task fails with "Spark History Server log download failed
  ({status_code})"**, the History Server rejected the `GET .../logs`
  request, e.g. the app_id/attempt_id doesn't exist or the server is
  unreachable/misconfigured. Check `base_url`, `app_id` and `attempt_id`,
  and hit the same URL directly to see the History Server's own error.
- **Task fails with "Spark History Server log download for app {app_id}
  exceeded {timeout}s"**, the download's total wall-clock transfer time
  exceeded `HistoryServerLogSourceHook`'s `timeout`, even though no single
  read/connect stalled (a slow-trickling connection). Raise `timeout` for
  large event logs, or check network throughput to the History Server.
- **Task fails with "Unexpected Spark History Server log archive contents
  for app {app_id}"**, the History Server's log zip was neither a single
  bare event-log file nor a rolling log whose `events_<n>_...` entries all
  sit directly under one `eventlog_v2_<appId>/` folder (the layout Spark's
  History Server writes). "not under exactly one folder" means the entries
  are flat, nested deeper, or split across folders (e.g. several attempts:
  set `attempt_id`); "has no events_<n>_ rolling-log entries" means the one
  folder holds no rolling segments. Raised directly during
  `resolve()`, before `sparkforensics-analyze` ever runs; check the exception
  message's entry-name list against what the History Server actually
  returned.
- **Task fails with "Configured log path does not exist"**,
  `FilesystemLogSourceHook`'s `path_template` rendered to a path that isn't
  there, e.g. a wrong `ds`/`run_id`/`dag_id` substitution, or the log
  hasn't landed yet. Check the exception message's rendered path against
  the filesystem/mount.
- **Task fails with "Configured log path does not exist (checked over
  SFTP...)"**, `SFTPLogSourceHook`'s `path_template` rendered to a remote
  path `SFTPHook.path_exists()` reports missing. Check the exception
  message's rendered path and `ssh_conn_id` against the on-prem host.
- **Task fails with "SFTP log fetch failed (ssh_conn_id=...)"**, an
  SSH/SFTP transport-level failure (auth failure, host unreachable,
  network timeout) during `SFTPLogSourceHook.resolve()`. Check the
  exception message's `ssh_conn_id` and remote path against the
  Airflow Connection and on-prem host; the wrapped underlying error is in
  the message.
- **Task fails with "SSH tunnel setup failed (ssh_conn_id=..., remote=...)"**,
  the same kind of transport-level failure, but establishing the SSH
  tunnel `SSHTunneledLogSourceHook` needs before it can build/call its
  wrapped hook. Check `ssh_conn_id` and `remote_host:remote_port` against
  the on-prem host.
- **Task fails with "SSH tunnel teardown failed after a successful
  fetch (ssh_conn_id=...)"**, the wrapped hook's `resolve()` already
  succeeded, but closing the SSH tunnel afterward raised. The fetched
  result is lost even though the log was retrieved; check `ssh_conn_id`
  against the on-prem host for a mid-task disconnect, then rerun the
  task.
- **Task fails with "path_template resolved to a remote path/directory
  with no name component..."**, `SFTPLogSourceHook`'s `path_template`
  rendered to a path ending at the filesystem root (e.g. a template bug
  reduces to `"/"`), leaving nothing to name the staged local file or
  directory. Check the rendered path in the exception message against
  `path_template` and the substituted context variables.
- **A task using `SSHTunneledLogSourceHook` hangs instead of failing**,
  `SSHHook.get_tunnel()`'s SSH-level connection setup has no documented
  connect timeout of its own; if the on-prem host is unreachable at the
  SSH layer (as opposed to the wrapped HTTP hook's own `timeout`), tunnel
  establishment can hang rather than raise, tying up a worker slot. Bound
  this with the task's own `execution_timeout` until/unless a lower-level
  connect timeout is confirmed available on the `SSHHook` side.
- **`ThresholdBreached` raised**, expected behavior when
  `on_threshold_breach="fail"` and a configured threshold was violated. The
  exception message lists every breached threshold's name and detail.
  `ThresholdBreached` is an `AirflowFailException`, so it does not consume
  the task's configured retries: a breach is a deterministic verdict from
  the completed analysis, and retrying would only reach the same result.
- **A threshold shows as "inconclusive" every run**, the event log lacks
  the evidence that threshold needs (e.g. no `ApplicationEnd`, no
  trustworthy task-level metrics). Logged as a warning regardless of
  `on_threshold_breach`; this is not itself a failure condition.
- **The "SparkForensics report" link in the UI is blank**, on Airflow 2.x
  the webserver reads the link from XCom and shows an empty link if that
  read fails, rather than breaking the task page. Check the webserver log
  for a "ReportLink could not read the report destination from XCom"
  error and its traceback. The report itself was still persisted and is
  still in XCom under `return_value`. On Airflow 3.x the worker computes
  the link after the task runs, from the destination the run actually
  persisted. The link is blank by design when the task failed before
  persisting a report (for example the event log fetch or the analysis
  raised); check the task log for that error. A failure computing the
  link itself shows in the task log as "Failed to push an xcom for task
  operator extra link".
- **The "SparkForensics report" link is missing from the task page
  entirely (Airflow 2.x)**, the webserver renders from the serialized DAG,
  and Airflow drops `ReportLink` during deserialization unless the
  package's `airflow.plugins` entry point is registered. The DAG
  processor/scheduler log then shows "Operator Link class
  'sparkforensics_operator.links.ReportLink' not registered". Make sure
  `sparkforensics-operator` is installed (as a package, not only copied
  onto the DAGs folder) on every process that serializes or renders the
  DAG, not only on workers, and check `airflow plugins` lists
  `sparkforensics_operator`.
- **Disk fills up on a worker that runs this operator repeatedly**, a
  single event log can be hundreds of MB to several GB, so this matters on
  a busy worker. Each `LogSourceHook` now cleans up after itself, but only
  a path it created:
  - `HistoryServerLogSourceHook` with no `dest_dir` downloads to a private
    temp dir and removes it automatically after analysis, including if `resolve()` itself fails partway through.
  - `FilesystemLogSourceHook` with `dest_dir` set copies the log there and
    removes that copy automatically after analysis.
  - `HistoryServerLogSourceHook` with `dest_dir` set writes into a
    caller-managed shared directory the hook doesn't own, so it is
    **not** auto-cleaned. Point it at a scratch volume with its own
    external cleanup (e.g. `tmpwatch` or a cron job).
  - `FilesystemLogSourceHook` with no `dest_dir` returns the caller's own
    event log path unmodified; this package never deletes it.
  - `SFTPLogSourceHook` with no `dest_dir` downloads to a private temp dir
    and removes it automatically after analysis, including if `resolve()`
    itself fails partway through.
  - `SFTPLogSourceHook` with `dest_dir` set writes into a caller-managed
    shared directory the hook doesn't own, so it is **not** auto-cleaned.
    Same external-cleanup advice as `HistoryServerLogSourceHook`'s
    `dest_dir` case above.
  - `SSHTunneledLogSourceHook` has no cleanup behavior of its own: it
    delegates `cleanup()` to whatever inner hook `hook_factory` built, so
    the wrapped hook's own entry above (e.g.
    `HistoryServerLogSourceHook`'s) applies.
  - `RemotePathLogSourceHook` and `HistoryServerAppLogSourceHook` write
    nothing to the worker. `SSHAnalyzeHook` reads the report from stdout,
    so it leaves no file on the SSH host either.
