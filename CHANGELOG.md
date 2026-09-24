# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
