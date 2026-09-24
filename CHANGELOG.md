# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  can defer. Needs a triggerer with the `ssh` extra installed.
- `SSHAnalyzeHook(remote_base_dir=..., poll_interval=...)` for the
  deferrable mode's job directory and trigger poll interval.
- `DeferrableAnalyzeHook`, the interface a backend implements to support
  `deferrable=True`, exported from the top-level package.

### Changed

- Breaking: `LogSourceHook.fetch(context) -> Path` is now
  `LogSourceHook.resolve(context) -> EventLogRef`, and
  `LogSourceHook.cleanup()` receives that reference. The fetching hooks
  return a `LocalEventLog`.
- Breaking: `AnalyzeHook.analyze()` takes an `EventLogRef` instead of a
  path. Custom backends set `supported_log_refs` and implement
  `_analyze()`; `analyze()` rejects a reference kind the backend can't read
  before running anything.
- `SSHTunneledLogSourceHook` rejects a wrapped hook that returns anything
  but a `LocalEventLog`, since the tunnel is closed by the time the
  reference would be used.
- Breaking: the `ssh` extra requires `apache-airflow-providers-ssh>=6.0.1`
  (was `>=5.0`), the first release whose remote-job helpers quote paths.
- `spark_forensics_callback(deferrable=True)` raises `ValueError`, since a
  callback has no task to defer.
- The exit-2 error now also names a failed History Server fetch as a
  cause, and a report that isn't valid JSON raises `AirflowException`
  instead of a bare `JSONDecodeError`.

### Fixed

- Install instructions and the binary-not-found error now name the
  `sparkforensics-cli` npm package and require Node.js 18+.
- `__version__` is read from the installed package metadata instead of a
  hardcoded string that had drifted to 0.1.0.
- README links are absolute, so they work on the PyPI project page.
- The "SparkForensics report" link now shows in the Airflow 2.x UI.
  `ReportLink` is registered through an `airflow.plugins` entry point, so
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

[0.1.1]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.1.1
[0.1.0]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.1.0
