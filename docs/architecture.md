# Architecture

One operator, pluggable strategy Hooks (not config-driven branching): three
`LogSourceHook` implementations (Spark History Server REST API, filesystem/
mounted-HDFS path pattern, XCom) and one `AnalyzeHook` implementation
(subprocess against the `sparkforensics-analyze` CLI). Both trigger shapes (the standalone `SparkForensicsOperator` and the `spark_forensics_callback`
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
- `hooks/log_source/{base,history_server,filesystem,xcom}.py`, fetch the
  event log to a local path. `LogSourceHook.cleanup(path)` (no-op by
  default) lets a hook remove a path it created for itself once analysis
  is done; `run_spark_forensics` calls it in a `finally`, so it runs on
  both success and failure *after* `fetch()` has returned. That `finally`
  does not cover a `fetch()` that raises before returning (there is no
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
  `hooks/log_source/_path_template.py` and `_rolling_log.py` hold the
  path-templating and rolling-log-entry-matching logic shared between
  `filesystem.py`/`sftp.py` and `history_server.py`/`sftp.py`
  respectively. `_dest_root.py` holds the `dest_root/<basename>` staging
  convention shared by `filesystem.py`/`sftp.py`, and the owned-temp-dir
  bookkeeping (create if `dest_dir` is unset, `rmtree` on cleanup/error if
  owned) shared by `sftp.py`/`history_server.py`.
- `hooks/analyze/{base,subprocess}.py`, run sparkforensics analysis over
  that local path. v1 ships the subprocess backend only (see "Deferred"
  below).
- `report.py`, `Report`/`ThresholdResult` dataclasses, plus parsing of the
  CLI's JSON `--out` file and its stderr threshold lines. No
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
  `run_spark_forensics` core.
- `callback.py`, `spark_forensics_callback(**kwargs)`, an
  `on_success_callback` factory sharing the same core.

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
worth doing: `hooks/analyze/mcp.py`, using an MCP Python client (e.g.
`mcp` on PyPI) against `POST {server_url}/mcp`, calling `resolveOrCreateRun`
then `diagnoseRun` + `evaluateBudgetsForRun`.

**Deferrable execution.** Both a slow `subprocess` analyze run and a future
`http` backend currently block a worker slot for the full analyze duration.
See the design spec's own "Future: deferrable execution" section for the
three options and why none is needed yet.
