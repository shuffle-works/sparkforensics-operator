# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-25

### Added

- Remote analysis: `SSHAnalyzeHook` runs `sparkforensics-analyze` on the
  host behind an Airflow SSH connection, such as the Spark History Server's
  node, and reads the JSON report from its stdout. The event log never
  reaches the Airflow worker, and the worker no longer needs Node.js.
  Requires the `ssh` extra.
- `RemotePathLogSourceHook`, an event log path on an SSH host, left in
  place for `SSHAnalyzeHook`.
- `HistoryServerAppLogSourceHook`, a History Server application the CLI
  fetches itself through `--shs-base-url`/`--app-id`/`--attempt-id`,
  instead of downloading it to the worker first. Works with both backends.
- Event log references (`LocalEventLog`, `RemoteEventLog`,
  `HistoryServerApp`), exported from the top-level package.
- Deferrable remote analysis: `SparkForensicsOperator(deferrable=True)`
  with `SSHAnalyzeHook` submits the CLI as a detached job on the SSH host
  through the SSH provider's remote-job wrapper, defers on its
  `SSHRemoteJobTrigger`, and resumes to read the report file back, so no
  worker slot is held while the analysis runs. Deferred and synchronous
  runs persist the same report and raise the same errors. `deferrable`
  left unset follows `[operators] default_deferrable` for backends that
  can defer. Needs a triggerer with the `ssh` extra installed, and
  `apache-airflow-providers-ssh>=6.0.1` (Airflow 2.11+): with an older
  provider `deferrable=True` raises `ValueError` at DAG parse. Each task
  instance's job directory is keyed on the deployment's `base_url` too,
  so deployments sharing an SSH user never stop each other's jobs, and
  the submitted job is kept in XCom (`sparkforensics_remote_job`) so a
  failed, timed-out or killed deferral stops the job it submitted.
- `SSHAnalyzeHook(remote_base_dir=..., poll_interval=...)` for the
  deferrable mode's job directory and trigger poll interval.
- `DeferrableAnalyzeHook`, the interface a backend implements to support
  `deferrable=True`, exported from the top-level package.
- `SparkForensicsOperator.on_kill()`: killing, clearing or marking the
  task failed while a worker runs it stops the remote analysis. A
  deferrable task's remote job is stopped and removed; a synchronous
  `SSHAnalyzeHook` run closes its channel and stops the remote CLI, which
  closing the channel alone never did. Backends get an `on_kill()` hook,
  a no-op by default. Hosts without procps (`pkill`) are handled: the
  job's session is found in `/proc`.
- Templated hook arguments: `log_source` and `backend` are template
  fields of `SparkForensicsOperator`, and every hook lists what it renders
  in its own `template_fields` (paths, connection ids, History Server app
  and attempt ids, the CLI binary, `remote_base_dir`). Each task renders
  its own copy of a shared hook. `spark_forensics_callback` renders
  `report_dest` and its hooks itself, with the callback's context.
- A summary XCom, `sparkforensics_summary`: impact-band counts, whether a
  threshold was violated, the breached and inconclusive threshold names,
  the schema version, the destination and the report URL. Pushed before
  notifying and before a breach raises, the same on synchronous and
  deferred runs. `return_value` is unchanged.
- `report_url_template` on the operator and the callback: a browser URL
  built from the destination (`{destination}`, `{bucket}`, `{key}`,
  `{path}`) that `ReportLink` opens instead of the raw destination.
  Nothing is presigned.
- Airflow provider metadata (`get_provider_info`, the
  `apache_airflow_provider` entry point and the `Framework :: Apache
  Airflow :: Provider` classifier), so `airflow providers list` shows the
  package.

### Changed

- Breaking: `LogSourceHook.fetch(context) -> Path` is now
  `LogSourceHook.locate(context) -> EventLogRef`, and
  `LogSourceHook.cleanup()` receives that reference. The fetching hooks
  return a `LocalEventLog`.
- Breaking: `path_template` is a Jinja template. The `{ds}`, `{run_id}`,
  `{dag_id}`, `{task_id}` and `{logical_date}` placeholders are rejected
  with an error naming the Jinja form (`{run_id}` becomes
  `{{ run_id }}`). Instead of reducing each value to its basename, a
  rendered path with a `..` segment is rejected.
- Breaking: the History Server hooks' `app_id` and `attempt_id` are
  rendered like any other hook argument; an `app_id` that renders to `""`
  or `"None"` raises.
- Every Airflow import goes through the Task SDK location where the
  running Airflow has one, so importing the package emits no Airflow
  deprecation warnings.
- Breaking: `AnalyzeHook.analyze()` takes an `EventLogRef` instead of a
  path. Custom backends set `supported_log_refs` and implement
  `_analyze()`; `analyze()` rejects a reference kind the backend can't read
  before running anything.
- `SSHTunneledLogSourceHook` rejects a wrapped hook that returns anything
  but a `LocalEventLog`, since the tunnel is closed by the time the
  reference would be used.
- The `ssh` extra requires `apache-airflow-providers-ssh>=3.7.1` (was
  `>=5.0`, which needs Airflow 2.11), so `SSHAnalyzeHook`'s synchronous
  mode, `SFTPLogSourceHook` and `SSHTunneledLogSourceHook` work from
  Airflow 2.6 on, the package's own floor. Only the deferrable mode needs
  `apache-airflow-providers-ssh>=6.0.1`, the first release whose
  remote-job helpers quote paths.
- A synchronous `SSHAnalyzeHook` run starts the CLI in its own session
  (under `setsid` when the host has it) and reports its pid on the
  channel, so `on_kill()` can stop it; it still writes nothing on the SSH
  host. Its SSH transport error names the step and the log instead of
  quoting the whole remote command.
- `spark_forensics_callback(deferrable=True)` raises `ValueError`, since a
  callback has no task to defer.
- The exit-2 error now also names a failed History Server fetch as a
  cause, and a report that isn't valid JSON raises `AirflowException`
  instead of a bare `JSONDecodeError`.

### Removed

- Breaking: `sparkforensics_operator.report.parse_report_json()`. The
  analyze backends parse the CLI's JSON report internally; there is no
  replacement.

### Fixed

- Install instructions and the binary-not-found error now name the
  `sparkforensics-cli` npm package and require Node.js 18+.
- `__version__` is read from the installed package metadata instead of a
  hardcoded string that had drifted to 0.1.0.
- README links are absolute, so they work on the PyPI project page.
- The "SparkForensics report" link now shows in the Airflow 2.x UI.
  `ReportLink` is registered as an extra link in the provider metadata, so
  it survives DAG serialization instead of being dropped as "not
  registered".
- On Airflow 3.x, `ReportLink` returns the destination the run persisted
  its report to (recorded by `execute()` on the operator, including on a
  threshold breach, and empty when the run failed before persisting) when
  the task runner computes the link on the worker, instead of querying the
  metadata database through `airflow.models.xcom.XComModel`. A failed XCom
  read on Airflow 2.x is now logged as an error with its traceback.

## [0.1.1] - 2026-09-20

No functional changes. Republished after a repository infrastructure
migration, to verify the PyPI trusted-publishing pipeline against the new
repository identity.

## [0.1.0] - 2026-09-06

Initial release.

### Added

- `SparkForensicsOperator`: after a Spark job runs, fetches its event log
  and runs it through the `sparkforensics-analyze` CLI, then
  persists/XComs/threshold-checks/notifies on the result.
- Log source hooks: `FilesystemLogSourceHook`, `HistoryServerLogSourceHook`,
  `SFTPLogSourceHook`, `SSHTunneledLogSourceHook`, `XComLogSourceHook`.
- `SubprocessAnalyzeHook`, shelling out to the `sparkforensics-analyze` CLI.
- Generic `Notifier` interface for threshold-breach alerting.
- PyPI trusted-publishing GitHub Actions workflow (OIDC, no stored token).

[Unreleased]: https://github.com/shuffle-works/sparkforensics-operator/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.2.0
[0.1.1]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.1.1
[0.1.0]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.1.0
