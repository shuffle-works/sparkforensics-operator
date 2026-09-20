import re

# Mirrors isRollingLogDirectory in sparkforensics's src/cli/collect-run.ts.
_ROLLING_ENTRY_RE = re.compile(r"^events_\d+_")
