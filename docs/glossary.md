# Glossary

- **Event log**, the file (or, for a long-running app, directory of
  rolling segments named `events_<n>_...`) Spark writes describing a
  job's execution, the raw input to sparkforensics analysis.
- **Event log reference (`EventLogRef`)**, what a log source locates and
  a backend analyzes: `LocalEventLog` (on the worker), `RemoteEventLog` (on
  an SSH host) or `HistoryServerApp` (an application the CLI fetches from a
  History Server itself).
- **Backend / analysis host**, the `AnalyzeHook` that runs
  `sparkforensics-analyze` and the machine it runs it on: the Airflow
  worker for `SubprocessAnalyzeHook`, the SSH host for `SSHAnalyzeHook`.
- **Remote analysis**, running the analysis on an SSH host next to the
  logs with `SSHAnalyzeHook`, so the log never crosses the network and the
  worker needs no Node.js.
- **Deferred run**, a `SparkForensicsOperator(deferrable=True)` run: the
  analysis runs as a detached remote job while the task waits in the
  `deferred` state, polled by the triggerer, and a worker resumes the task
  to read the report back.
- **Remote job / job directory**, the detached CLI process of a deferred
  run and the directory on the SSH host holding its report, stderr, log
  and exit code, under `remote_base_dir/<task instance>/`, where the task
  instance directory is also keyed on the Airflow deployment's
  `base_url`. Removed once the report is read; a new try of the same task
  instance stops and removes any job an earlier try left there.
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
- **Report URL**, the browser address `ReportLink` opens instead of the
  raw destination, built from `report_url_template` and the destination's
  bucket, key or path. It carries no credentials of its own.
- **Summary XCom**, the dict pushed under the `sparkforensics_summary` key:
  impact-band counts, whether a threshold was violated, which thresholds
  were breached or inconclusive, the schema version, the destination and
  the report URL. Downstream tasks branch on it without reading the report.
- **Templated hook argument**, a hook attribute listed in its
  `template_fields`, rendered by Airflow with the task's context before
  the log is located or analyzed.
