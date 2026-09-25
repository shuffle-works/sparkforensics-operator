"""
What a run pushes to XCom besides the report destination: a small summary
downstream tasks can branch on without reading the report, and the
browser-openable report URL ReportLink shows.
"""
from __future__ import annotations

from urllib.parse import quote, urlparse

# XCom key of the summary; return_value stays the report destination.
SUMMARY_XCOM_KEY = "sparkforensics_summary"

_URL_FIELDS = ("destination", "bucket", "key", "path")


def _url_values(destination: str) -> dict:
    parsed = urlparse(destination)
    has_scheme = parsed.scheme not in ("", "file")
    values = {
        "destination": destination,
        "bucket": parsed.netloc if has_scheme else "",
        "key": parsed.path.lstrip("/") if has_scheme else "",
        "path": parsed.path if parsed.scheme else destination,
    }
    return {name: quote(value, safe="/") for name, value in values.items()}


def validate_report_url_template(template: str) -> None:
    """Raise ValueError unless template formats into an http(s) URL from the
    fields render_report_url() fills."""
    try:
        sample = template.format(**{name: "x" for name in _URL_FIELDS})
    except (KeyError, IndexError, ValueError) as e:
        raise ValueError(
            f"report_url_template {template!r} is not a valid template: {e!r}. It may use "
            f"{', '.join('{' + name + '}' for name in _URL_FIELDS)}."
        ) from e
    if urlparse(sample).scheme not in ("http", "https"):
        raise ValueError(
            f"report_url_template {template!r} must build an http:// or https:// URL."
        )


def render_report_url(destination: str | None, template: str | None) -> str | None:
    """The browser URL for a persisted report: template filled from
    destination, each value percent-encoded ("/" kept). None without a
    template or a destination; nothing here signs or grants access."""
    if not template or not destination:
        return None
    return template.format(**_url_values(destination))


def build_summary(report, *, destination: str, report_url: str | None, violated: bool) -> dict:
    counts = report.summary.get("impactBandCounts", {}) if isinstance(report.summary, dict) else {}
    return {
        "schema_version": report.schema_version,
        "destination": destination,
        "report_url": report_url,
        "impact_band_counts": {band: counts.get(band, 0) for band in ("critical", "warning", "info")},
        "violated": violated,
        "breached_thresholds": [r.name for r in report.threshold_results if r.status == "violation"],
        "inconclusive_thresholds": [r.name for r in report.threshold_results if r.status == "inconclusive"],
        "exit_code": report.exit_code,
    }
