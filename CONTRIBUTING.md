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

The suite needs no live Spark, Airflow or Node.js. The one real process is
a shell: the deferred-mode tests run the SSH provider's real shell wrapper against a local `sh` standing in
for the SSH host (`tests/hooks/analyze/_local_ssh.py`), so they need
`bash`, `setsid` and coreutils `timeout` on the machine running the tests.
The kill and sweep tests also run with procps hidden
(`LocalHost.hide_procps()`), as on slim images.

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
