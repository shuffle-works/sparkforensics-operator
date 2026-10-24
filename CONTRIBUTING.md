# Contributing

Design notes and the component map are in [ARCHITECTURE.md](ARCHITECTURE.md).
User documentation lives under [docs/](docs/index.md).

## Development install

```bash
pip install -e ".[test,s3,ssh]"
```

## Running the tests

```bash
pytest
```

The suite needs no live Spark or Airflow. Apart from `tests/integration`,
which skips without the CLI (below), it needs no Node.js either. The one
real process is a shell: the deferred-mode tests run the SSH provider's real shell wrapper against a local `sh` standing in
for the SSH host (`tests/hooks/analyze/_local_ssh.py`), so they need
`bash`, `setsid` and coreutils `timeout` on the machine running the tests.
The kill and sweep tests also run with procps hidden
(`LocalHost.hide_procps()`), as on slim images.

### Tests against the real CLI

The other tests simulate `sparkforensics-analyze`, so they cannot notice the
CLI changing an exit code or a flag's rules. `tests/integration` runs the
real one on public logs from the
[spark-event-corpus-data](https://github.com/shuffle-works/spark-event-corpus-data)
repository (fetched at a pinned tag), through `SubprocessAnalyzeHook`,
`SSHAnalyzeHook` against a local shell, and a fake History Server on
localhost. It covers a clean log, a violation, an unreadable log, an
unreadable baseline, budgets small enough to print in exponent form, several
regression budgets, a History Server baseline, `redact` and the Markdown
report.

```bash
npm install -g sparkforensics-cli
pytest tests/integration
```

The tests need Node.js 18+, the `sparkforensics-cli` package on `PATH` (or
its binary at `SPARKFORENSICS_ANALYZE_BIN`) and network access to GitHub, and
skip when either is missing. With `SPARKFORENSICS_REQUIRE_CLI=1` a missing CLI
or an unreachable corpus fails the run instead, as in CI. The `cli-integration`
job in `.github/workflows/ci.yml` installs Node.js and runs them against the
release the operator is tested against (`TESTED_CLI_VERSION` in
`report.py`) and the newest one.

## The tox Airflow matrix

`tox.ini` runs the suite against each supported Airflow major version:

```bash
tox -e py311-airflow2
tox -e py311-airflow3
```

The full env list is `py{39,310,311,312}-airflow2`,
`py{310,311,312}-airflow3`, and three floor envs:

- `py39-airflow2min` pins Airflow 2.6.3 without the `ssh` extra, and
  omits the SSH-only modules from its coverage (`coverage-no-ssh.ini`).
- `py39-airflow2sshmin` runs the `ssh` extra's floor
  (`apache-airflow-providers-ssh` 3.7.1) on Airflow 2.6.3. The deferrable
  tests skip there, since that mode needs a newer provider.
- `py311-airflow2deferrablemin` pins the deferrable mode's floors
  (`constraints-deferrable-floor.txt`).

The two Airflow 2.6.3 envs install through
`constraints-airflow-2.6-floor.txt` so the extras step can't upgrade
Airflow past the floor; `tox.ini` explains why.

CI (`.github/workflows/ci.yml`) runs a subset of these envs on every push
to `main` and every pull request. Each env writes coverage to
`coverage-<envname>.lcov`; CI uploads one file per env to Coveralls and
merges them in a `finish` job. `usedevelop = true` in `tox.ini` keeps the
lcov `SF:` paths pointing at `src/sparkforensics_operator/...` instead of
a `.tox` virtualenv, which Coveralls needs to attribute lines.

## Releasing to PyPI

`.github/workflows/publish.yml` builds the sdist and wheel and publishes
them on every GitHub Release (`release: published`). It uses PyPI trusted
publishing (OIDC), so no API token is stored in the repository.

One-time setup before the first release:

- On PyPI, add a trusted publisher for this project: owner
  `shuffle-works`, repo `sparkforensics-operator`, workflow `publish.yml`,
  environment `pypi`.
- In the GitHub repository settings, create an environment named `pypi`
  (optionally with required reviewers) to match.

To cut a release:

1. Bump `version` in `pyproject.toml` and move the `[Unreleased]` entries
   in `CHANGELOG.md` under the new version. The Airflow provider metadata
   reads the version from the installed package, so nothing else needs
   changing.
2. Tag the release commit (for example `v0.2.0`).
3. Publish a GitHub Release from that tag. The workflow does the rest.
