# Getting started

## Install

```bash
pip install sparkforensics-operator
```

The package supports Python 3.9 or newer and Airflow 2.6 or newer,
including Airflow 3. `airflow providers list` shows it, because the
package registers itself as a provider.

Optional extras:

| extra | install when | pulls in |
|---|---|---|
| `s3` | `report_dest` is an `s3://` URI | `apache-airflow-providers-amazon>=8.0` |
| `ssh` | you use `SFTPLogSourceHook`, `SSHTunneledLogSourceHook`, `RemotePathLogSourceHook` with `SSHAnalyzeHook`, or `SSHAnalyzeHook` itself | `apache-airflow-providers-ssh>=3.7.1`, `apache-airflow-providers-sftp>=4.0` |

```bash
pip install "sparkforensics-operator[s3,ssh]"
```

The `s3` extra uses the `aws_default` Airflow connection unless you pass
`aws_conn_id`. The SSH hooks reuse the `ssh_conn_id` Airflow connection an
`SSHOperator` already uses, so there is no new connection type to set up.

The `ssh` extra's floor works from Airflow 2.6 on. Only
[deferrable execution](configuration.md#deferrable-execution) needs more:
`apache-airflow-providers-ssh>=6.0.1`, which requires Airflow 2.11 or
newer. On an older provider the operator rejects `deferrable=True` when
the DAG is parsed and ignores `[operators] default_deferrable`.

The package ships no notifier. If you implement one (see
[Notifier](configuration.md#notifier)), install whatever it depends on,
such as a Slack provider or an SMTP library, yourself.

## Where the analysis runs

The analysis is the `sparkforensics-analyze` command from the
`sparkforensics-cli` npm package, version 0.4.0 or newer: the operator
reads the report schema 0.4.0 introduced and refuses an older CLI's
report. It needs Node.js 18 or newer (the npm package declares
`node >=18`). Install both on whichever host runs the
analysis, which depends on the backend you pick:

- `SubprocessAnalyzeHook` runs the CLI on the Airflow worker.
- `SSHAnalyzeHook` runs it on the host behind an Airflow SSH connection,
  and the worker needs neither Node.js nor the CLI.

### Worker prerequisites (`SubprocessAnalyzeHook`)

Install Node.js 18+ and the CLI on every worker that runs the task:

```bash
npm install -g "sparkforensics-cli@>=0.4.0"
```

`sparkforensics-analyze` must resolve on the worker's `PATH`. If it
doesn't, pass the full path as `SubprocessAnalyzeHook(analyze_bin=...)`.

### SSH host prerequisites (`SSHAnalyzeHook`)

`SSHAnalyzeHook` runs `sparkforensics-analyze` on the host behind its
`ssh_conn_id`, typically the node running the Spark History Server or one
that mounts the event log directory. The log is read there and never
crosses the network; only the JSON report comes back. Workers where
installing Node.js is awkward, such as Amazon MWAA, only need this package
with the `ssh` extra and network access to the SSH host.

On the SSH host:

- Install Node.js 18 or newer and run
  `npm install -g "sparkforensics-cli@>=0.4.0"`
  for the user `ssh_conn_id` logs in as.
- Check that the binary resolves in a non-interactive session, the kind
  `SSHAnalyzeHook` opens:
  `ssh <user>@<host> 'command -v sparkforensics-analyze'`.
  A non-interactive session often skips the profile that puts `nvm` or a
  custom npm prefix on `PATH`. If the command prints nothing, pass the
  full path from an interactive `command -v sparkforensics-analyze` as
  `SSHAnalyzeHook(analyze_bin=...)`.
- Coreutils `timeout` must be on that `PATH` too, since `SSHAnalyzeHook`
  wraps the CLI in it. Any standard Linux host has it.
- For `HistoryServerAppLogSourceHook`, `base_url` is resolved on this
  host, so `http://localhost:18080` reaches a History Server running on
  it. Check it with
  `ssh <user>@<host> 'curl -s http://localhost:18080/api/v1/applications?limit=1'`.
- For `RemotePathLogSourceHook`, the login user needs read access to the
  rendered path.
- For `deferrable=True`, the host also needs bash, and `setsid` to stop
  a job cleanly. See [deferred runs](#deferred-runs) below.

## Wire it into a DAG

Pick a log source (where the event log comes from) and a backend (where
the analysis runs). [Configuration](configuration.md#which-log-source-works-with-which-backend)
lists which pairs work.

### Standalone operator

Chain `SparkForensicsOperator` after the Spark task. A threshold breach
fails this task, and so the DAG run:

```python
from sparkforensics_operator import FilesystemLogSourceHook, SparkForensicsOperator, SubprocessAnalyzeHook

run_spark_job = SparkSubmitOperator(task_id="run_spark_job", ...)

check_spark_job = SparkForensicsOperator(
    task_id="check_spark_job",
    log_source=FilesystemLogSourceHook(path_template="/mnt/spark-logs/{{ run_id }}/eventlog"),
    backend=SubprocessAnalyzeHook(),
    report_dest="s3://reports/{{ run_id }}/report.json",
    max_runtime_ms=3_600_000,
    max_skew_ratio=3,
    on_threshold_breach="fail",
)

run_spark_job >> check_spark_job
```

### Callback on the Spark task

Attach `spark_forensics_callback` as the Spark task's
`on_success_callback` instead. Airflow logs and swallows any exception
raised inside a callback, so a breach here never fails or retries the
upstream task, whatever `on_threshold_breach` is set to:

```python
from sparkforensics_operator import SubprocessAnalyzeHook, XComLogSourceHook, spark_forensics_callback

run_spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    on_success_callback=spark_forensics_callback(
        log_source=XComLogSourceHook(task_id="run_spark_job"),
        backend=SubprocessAnalyzeHook(),
        report_dest="s3://reports/run_spark_job/app.json",
        max_runtime_ms=3_600_000,
    ),
    ...,
)
```

### Remote analysis over SSH

Run the analysis on the Spark History Server's own node, so the event log
never reaches the worker. `ssh_conn_id` is the Airflow SSH connection to
that node, and `base_url` is the History Server as seen from it:

```python
from sparkforensics_operator import HistoryServerAppLogSourceHook, SparkForensicsOperator, SSHAnalyzeHook

check_spark_job = SparkForensicsOperator(
    task_id="check_spark_job",
    log_source=HistoryServerAppLogSourceHook(
        base_url="http://localhost:18080",
        app_id="{{ ti.xcom_pull(task_ids='run_spark_job', key='app_id') }}",
    ),
    backend=SSHAnalyzeHook(ssh_conn_id="shs_node"),
    report_dest="s3://reports/{{ run_id }}/report.json",
    max_runtime_ms=3_600_000,
)
```

To analyze an event log path on that host instead, use
`RemotePathLogSourceHook(ssh_conn_id="shs_node", path_template="/spark-logs/{{ run_id }}")`
with the same `ssh_conn_id`.

### Deferred runs

Add `deferrable=True` to an operator that uses `SSHAnalyzeHook` to release
the worker slot while the analysis runs. The worker submits the CLI as a
detached job on the SSH host, the triggerer polls it over SSH, and a
worker picks the task up again to read the report back:

```python
SparkForensicsOperator(
    task_id="forensics",
    log_source=RemotePathLogSourceHook(ssh_conn_id="onprem_ssh", path_template="/spark-events/{{ run_id }}"),
    backend=SSHAnalyzeHook(ssh_conn_id="onprem_ssh", timeout=1800),
    report_dest="s3://reports/{{ run_id }}/app.json",
    max_spill_gb=10,
    deferrable=True,
)
```

On top of the SSH host prerequisites above, a deferred run needs:

- A running triggerer (`airflow triggerer`). Without one the task sits in
  the `deferred` state until its deferral times out.
- `sparkforensics-operator[ssh]` installed on the triggerer as well as on
  the workers, with `apache-airflow-providers-ssh>=6.0.1` (Airflow 2.11+).
  The trigger is the SSH provider's `SSHRemoteJobTrigger`, which needs
  that provider and `asyncssh` on the triggerer. Installing this package's
  extra keeps the triggerer on the same provider version the workers
  submit with.
- On managed Airflow, a version that runs a triggerer and meets the
  Airflow 2.11 floor. On Amazon MWAA, pick an environment version that has
  both (MWAA only runs a triggerer from Airflow 2.7 on) and add
  `sparkforensics-operator[ssh]` to its `requirements.txt`, which MWAA
  installs for the triggerer as well as the workers.
- An SSH connection the triggerer can use. The triggerer connects with
  `SSHHookAsync` (asyncssh), not the paramiko `SSHHook` the worker uses,
  from the same `ssh_conn_id`. It reads the connection's host, port,
  login, password and the `key_file`, `private_key`, `passphrase`,
  `known_hosts`, `host_key` and `no_host_key_check` extras; a `key_file`
  path must exist on the triggerer host. A connection that depends on
  other extras (a proxy command, for instance) works for the worker steps
  but not for the triggerer's polls.
- bash on the SSH host (the provider's job wrapper uses it), and `setsid`
  (util-linux) to stop a job cleanly. The job runs as its own session,
  and stopping it signals the whole session, coreutils `timeout` and the
  CLI included. It uses `pkill` (procps) for that when present, and
  otherwise finds the session's processes in `/proc`, so slim images
  without procps work too. Only on a host with neither is just the job's
  process group signalled, which misses the CLI; it then stops at its own
  `timeout`.
- A distinct `base_url` per Airflow deployment, if several deployments
  run the same DAGs as the same SSH user. See
  [job directories](configuration.md#job-directories).

[Troubleshooting](troubleshooting.md#deferred-runs) covers how retries,
timeouts and clears behave for a deferred run.

## Next steps

- [Configuration](configuration.md) documents every argument, hook and
  threshold.
- [Troubleshooting](troubleshooting.md) maps error messages to causes.
