# Glossary

- **Event log**, the file (or, for a long-running app, directory of
  rolling segments named `events_<n>_...`) Spark writes describing a
  job's execution, the raw input to sparkforensics analysis.
- **Event log reference (`EventLogRef`)**, what a log source resolves and
  a backend analyzes: `LocalEventLog` (on the worker), `RemoteEventLog` (on
  an SSH host) or `HistoryServerApp` (an application the CLI fetches from a
  History Server itself).
- **Backend / analysis host**, the `AnalyzeHook` that runs
  `sparkforensics-analyze` and the machine it runs it on: the Airflow
  worker for `SubprocessAnalyzeHook`, the SSH host for `SSHAnalyzeHook`.
- **Remote analysis**, running the analysis on an SSH host next to the
  logs with `SSHAnalyzeHook`, so the log never crosses the network and the
  worker needs no Node.js.
- **Spark History Server (SHS)**, the Spark UI's REST API for completed
  applications; `HistoryServerLogSourceHook` downloads event logs from its
  `/api/v1/applications/{app_id}/logs` endpoint, and
  `HistoryServerAppLogSourceHook` lets the CLI fetch them itself.
- **Finding**, one detected issue in a run (e.g. stage skew, spill,
  failed tasks), as reported by sparkforensics.
- **Threshold / budget**, a configured limit (`max_runtime_ms`,
  `max_spill_gb`, `max_skew_ratio`, `max_failed_task_rate_pct`,
  `min_efficiency_pct`) this package asks `sparkforensics-analyze` to
  enforce; upstream calls the same concept a "budget."
- **Threshold breach**, at least one configured threshold's status came
  back `"violation"`. What happens next is `on_threshold_breach`
  (`"fail"` raises `ThresholdBreached`, `"warn"` logs, `"ignore"` is
  silent).
- **Inconclusive**, a threshold couldn't be evaluated at all (missing
  evidence in the event log), distinct from a pass or a violation. Always
  logged as a warning, never itself a threshold breach.
- **Report destination**, where `sinks.persist()` wrote the report JSON
  (a local path, `file://` path, or `s3://` URI); this is what's returned
  from `execute()`/the callback, auto-pushed to XCom, and shown by
  `ReportLink`. On Airflow 3.x `ReportLink` reads it from the operator's
  `persisted_report_dest` instead of XCom; it stays empty when the run
  failed before a report was persisted.
