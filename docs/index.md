# sparkforensics-operator documentation

sparkforensics-operator is an Airflow package that runs
[sparkforensics](https://github.com/shuffle-works/sparkforensics) analysis
on a Spark job's event log after the job finishes. It fetches or locates
the event log, runs the `sparkforensics-analyze` CLI on the Airflow worker
or on an SSH host, persists the JSON report, pushes a summary to XCom,
calls an optional notifier, and can fail the task when a configured
threshold is breached.

## Pages

- [Getting started](getting-started.md): install, extras, the Node.js and
  `sparkforensics-cli` prerequisite on the host that runs the analysis,
  and example DAG wiring.
- [Configuration](configuration.md): every operator argument, the
  callback factory, log source and analyze hooks, thresholds, the
  notifier, templated arguments, the summary XCom and report
  destinations.
- [Troubleshooting](troubleshooting.md): error messages and what causes
  them, deferred-run failure modes, disk usage and the report link.

For contributors:

- [CONTRIBUTING.md](../CONTRIBUTING.md): development install, tests, the
  tox Airflow matrix and the release procedure.
- [ARCHITECTURE.md](../ARCHITECTURE.md): components and design decisions.
- [CHANGELOG.md](../CHANGELOG.md): release history.
