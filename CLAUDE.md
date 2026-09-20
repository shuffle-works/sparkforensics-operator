# sparkforensics-operator

Airflow package: after a Spark job runs, fetches its event log and runs it
through the sparkforensics-analyze CLI, then persists/XComs/threshold-checks/
notifies on the result.

## Stack
Python 3.9+, apache-airflow 2.6+ or 3.0+, pytest. Runtime dependency: Node.js
(engines `>=22.18.0 <23.0.0 || >=23.6.0`) + the `sparkforensics` npm package
on the worker, for the `sparkforensics-analyze` CLI the subprocess backend
shells out to.

## Commands
- `pip install -e ".[test,s3,ssh]"`, install for local dev.
- `pytest`, run the test suite (mocks everything: no live Spark/Airflow/Node
  needed for any single test).
- `tox -e py311-airflow2` / `tox -e py311-airflow3`, run the suite against a
  specific Airflow major version.

## Design docs
- `docs/superpowers/specs/2026-09-05-sparkforensics-operator-design.md`, the original design spec.
- `docs/superpowers/plans/2026-09-05-sparkforensics-operator.md`, the
  implementation plan, including every upstream sparkforensics CLI/API
  fact and every Airflow-2-vs-3 compat fact this codebase depends on.
- `docs/architecture.md`, `docs/api-reference.md`, `docs/runbook.md`,
  `docs/glossary.md`, see below.
