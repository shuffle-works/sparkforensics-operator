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

Each tox env writes coverage to `coverage-<envname>.lcov` (see `tox.ini`); CI
uploads these per-matrix-env to Coveralls and merges them in a `finish` job.
`usedevelop = true` in `tox.ini` is required for the lcov `SF:` paths to match
the repo tree (`src/sparkforensics_operator/...`) instead of a `.tox` venv
path, which Coveralls needs to attribute lines.

## History

This repo's git history was squashed to a single commit on 2026-09-20 ahead
of going public. The original multi-PR history (42 commits, 4 PRs) lives in
`shuffle-works/sparkforensics-operator-archive-private` (private, kept
indefinitely). `git blame`/`git log -p` on any line predating that date stop
at the squash commit; check the archive repo instead.

## Design docs
- `docs/superpowers/specs/2026-09-05-sparkforensics-operator-design.md`, the original design spec.
- `docs/superpowers/plans/2026-09-05-sparkforensics-operator.md`, the
  implementation plan, including every upstream sparkforensics CLI/API
  fact and every Airflow-2-vs-3 compat fact this codebase depends on.
- `docs/architecture.md`, `docs/api-reference.md`, `docs/runbook.md`,
  `docs/glossary.md`, see below.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
