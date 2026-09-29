# sparkforensics-cli 0.4.0 output

Unedited output of `sparkforensics-analyze` built from sparkforensics
`main` at e6f01b9 (the 0.4.0 release content), run with `--redact` on logs
from the public corpus
[spark-event-corpus-data](https://github.com/shuffle-works/spark-event-corpus-data)
(`logs/` at 73ba122). Each `.json` is the `--out` file and each `.stderr`
the CLI's stderr.

| files | command |
|---|---|
| `report.*` (exit 1) | `sparkforensics-analyze failure-stage-abort.ndjson --redact --out report.json --max-skew 2 --max-runtime 1 --max-spill 100` |
| `comparison.*` (exit 1) | `sparkforensics-analyze pairwise-02.ndjson --baseline pairwise-01.ndjson --redact --out comparison.json --max-regression-pct 1` |
| `incomplete-run.*` (exit 3) | `sparkforensics-analyze failure-killed-run.ndjson --redact --out incomplete-run.json` |

Regenerate them with the same commands when the operator moves to a newer
report schema.
