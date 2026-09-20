# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/shuffle-works/sparkforensics-operator/releases/tag/v0.1.0
