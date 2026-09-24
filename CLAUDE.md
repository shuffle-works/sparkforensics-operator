# sparkforensics-operator

Airflow package: after a Spark job runs, fetches its event log and runs it
through the sparkforensics-analyze CLI, then persists/XComs/threshold-checks/
notifies on the result.

## Stack
Python 3.9+, apache-airflow 2.6+ or 3.0+, pytest. Runtime dependency: Node.js
18+ + the `sparkforensics-cli` npm package (`npm install -g sparkforensics-cli`)
on the worker, for the `sparkforensics-analyze` CLI the subprocess backend
shells out to.

## Commands
- `pip install -e ".[test,s3,ssh]"`, install for local dev.
- `pytest`, run the test suite (mocks everything: no live Spark/Airflow/Node
  needed for any single test).
- `tox -e py311-airflow2` / `tox -e py311-airflow3`, run the suite against a
  specific Airflow major version.

## Design docs
- `docs/architecture.md`, `docs/api-reference.md`, `docs/runbook.md`,
  `docs/glossary.md`, see below.
