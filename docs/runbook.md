# Runbook

## Prerequisites on the worker

- Node.js 18 or newer (the `sparkforensics-cli` package declares
  `node >=18`).
- The `sparkforensics-cli` npm package installed
  (`npm install -g sparkforensics-cli`), with `sparkforensics-analyze`
  resolvable on `PATH`, or pass `SubprocessAnalyzeHook(analyze_bin=<full path>)`.
- `pip install sparkforensics-operator[s3]` if `report_dest` is `s3://...`.
  Uses the `aws_default` Airflow connection unless `aws_conn_id` is set.
- `pip install sparkforensics-operator[ssh]` if using `SFTPLogSourceHook`
  or `SSHTunneledLogSourceHook`. Both reuse the `ssh_conn_id` Airflow
  Connection already configured for `SSHOperator`: no new connection
  type to set up. Note: this extra's floor
  (`apache-airflow-providers-ssh>=5.0`) transitively requires
  `apache-airflow>=2.11`, higher than this package's own overall
  `apache-airflow>=2.6` floor.
- Any dependency your own `Notifier` implementation needs (e.g. a
  provider package for Slack/Teams/PagerDuty, an SMTP library) is on you
  to install; this package declares none for notification.

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
- **Task fails with "sparkforensics-analyze failed to parse the event log or
  was given bad arguments (exit 2)"**, the fetched log isn't a valid Spark
  event log/rolling-log directory, or an unsupported CLI argument was
  passed. Check the raised `AirflowException`'s message, which includes the
  CLI's stderr.
- **Task fails with "sparkforensics-analyze exited with unexpected code
  {returncode}"**, the CLI exited with a code other than 0, 1, 2 or 3 (e.g.
  an OOM kill or a wrapper-script failure). The `--out` file is not trusted
  for these codes; check the exception message's stderr fragment for the
  underlying cause.
- **Task fails with "sparkforensics-analyze timed out after {timeout}s"**,
  the CLI didn't finish within `SubprocessAnalyzeHook`'s configured
  `timeout` (default 900s). Raise `timeout` for large or rolling event
  logs, or check the worker for resource contention.
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
  for app {app_id}"**, the History Server's log zip had neither a single
  entry nor rolling-log (`events_<n>_...`) entries, an archive layout
  `HistoryServerLogSourceHook` doesn't recognize. Raised directly during
  `fetch()`, before `sparkforensics-analyze` ever runs; check the exception
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
  network timeout) during `SFTPLogSourceHook.fetch()`. Check the
  exception message's `ssh_conn_id` and remote path against the
  Airflow Connection and on-prem host; the wrapped underlying error is in
  the message.
- **Task fails with "SSH tunnel setup failed (ssh_conn_id=..., remote=...)"**,
  the same kind of transport-level failure, but establishing the SSH
  tunnel `SSHTunneledLogSourceHook` needs before it can build/call its
  wrapped hook. Check `ssh_conn_id` and `remote_host:remote_port` against
  the on-prem host.
- **Task fails with "SSH tunnel teardown failed after a successful
  fetch (ssh_conn_id=...)"**, the wrapped hook's `fetch()` already
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
- **The "SparkForensics report" link in the UI is blank**, `ReportLink`
  reads XCom via an Airflow-internal API (not the stable `airflow.sdk`
  surface) and degrades to an empty link on any failure rather than
  breaking the UI. Check the scheduler/webserver/API-server log for a
  "ReportLink could not read the report destination from XCom" warning: that's this package's own internal-API guard, not silent data loss (the
  report itself was still persisted and is still in XCom under
  `return_value`).
- **The "SparkForensics report" link is blank only in the UI, never in
  logs**, `ReportLink` runs in the webserver/API-server process, not just
  the worker. If `sparkforensics-operator` is installed on workers but not
  on the webserver/API-server, `get_link` fails there and shows the same
  blank-link symptom above even though tasks run fine. Make sure the
  package is installed on every process that renders the DAG UI, not only
  on workers.
- **Disk fills up on a worker that runs this operator repeatedly**, a
  single event log can be hundreds of MB to several GB, so this matters on
  a busy worker. Each `LogSourceHook` now cleans up after itself, but only
  a path it created:
  - `HistoryServerLogSourceHook` with no `dest_dir` downloads to a private
    temp dir and removes it automatically after analysis, including if `fetch()` itself fails partway through.
  - `FilesystemLogSourceHook` with `dest_dir` set copies the log there and
    removes that copy automatically after analysis.
  - `HistoryServerLogSourceHook` with `dest_dir` set writes into a
    caller-managed shared directory the hook doesn't own, so it is
    **not** auto-cleaned. Point it at a scratch volume with its own
    external cleanup (e.g. `tmpwatch` or a cron job).
  - `FilesystemLogSourceHook` with no `dest_dir` returns the caller's own
    event log path unmodified; this package never deletes it.
  - `SFTPLogSourceHook` with no `dest_dir` downloads to a private temp dir
    and removes it automatically after analysis, including if `fetch()`
    itself fails partway through.
  - `SFTPLogSourceHook` with `dest_dir` set writes into a caller-managed
    shared directory the hook doesn't own, so it is **not** auto-cleaned.
    Same external-cleanup advice as `HistoryServerLogSourceHook`'s
    `dest_dir` case above.
  - `SSHTunneledLogSourceHook` has no cleanup behavior of its own: it
    delegates `cleanup()` to whatever inner hook `hook_factory` built, so
    the wrapped hook's own entry above (e.g.
    `HistoryServerLogSourceHook`'s) applies.
