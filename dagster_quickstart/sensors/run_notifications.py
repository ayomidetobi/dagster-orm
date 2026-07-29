"""Email notifications for run outcomes, using email_templates/*.html and
OutlookEmailResource.

OutlookEmailResource is a demo resource at the moment (no real SMTP mailbox
wired up -- see resources/outlook_email_resource.py and the "email" resource
in definitions.py) -- these sensors exercise the full path (real Dagster
event data -> template context -> send_email) so pointing it at a real
mailbox later is a config change, not a rewrite. Both sensors default to
STOPPED so nothing sends automatically until turned on.

Three states, mapped from real Dagster primitives -- there's no native
"warning" run status, so it's derived from asset check results:
  - run FAILURE                               -> run_failed.html
  - run SUCCESS, every asset check passed      -> run_succeeded.html
  - run SUCCESS, some asset check didn't pass  -> run_warning.html
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import markdown
from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)
from decouple import Csv, config
from jinja2 import Template

from dagster_quickstart.resources.outlook_email_resource import OutlookEmailResource

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "email_templates"


def _render(template_name: str, **context: Any) -> str:
    """Render one of email_templates/*.html with the given Jinja2 context."""
    return Template((TEMPLATES_DIR / template_name).read_text()).render(**context)


def _run_url(run_id: str) -> str:
    base = config("DAGSTER_WEBSERVER_URL", default="http://localhost:3000").rstrip("/")
    return f"{base}/runs/{run_id}"


def _format_utc(timestamp: float | None) -> str:
    if timestamp is None:
        return "unknown"
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_duration(start: float | None, end: float | None) -> str:
    if start is None or end is None:
        return "unknown"
    total_seconds = int(end - start)
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds}s" if minutes else f"{seconds}s"


def _triggered_by(tags: dict[str, str]) -> str:
    if tags.get("dagster/sensor_name"):
        return f"Sensor · {tags['dagster/sensor_name']}"
    if tags.get("dagster/schedule_name"):
        return f"Schedule · {tags['dagster/schedule_name']}"
    return "Manual"


def _parse_markdown_table(
    markdown_text: str, *, max_rows: int = 10, max_columns: int = 8
) -> tuple[list[str], list[list[str]]]:
    """Parse a GitHub-flavored markdown table (e.g. from DataFrame.to_markdown()) into (columns, rows).

    Uses the `markdown` package's "tables" extension to convert to real HTML
    first, then reads that back with ElementTree -- handles table syntax
    edge cases (escaped pipes, alignment markers, etc.) a hand-rolled
    "split on |" parser would get wrong. Capped for an email-sized preview --
    MaterializeResult metadata can carry a much bigger table (e.g.
    ingest_bloomberg_values reports every fetched series/timestamp) than an
    email should ever try to render. Returns ([], []) if `markdown_text`
    doesn't parse into a table.
    """

    html = markdown.markdown(markdown_text, extensions=["tables"])

    try:
        table = ElementTree.fromstring(html)
    except ElementTree.ParseError:
        return [], []

    if table.tag != "table":
        return [], []

    thead = table.find("thead")
    header_row = thead.find("tr") if thead is not None else None
    if header_row is None:
        return [], []
    columns = ["".join(cell.itertext()).strip() for cell in header_row.findall("th")][:max_columns]

    tbody = table.find("tbody")
    body_rows = tbody.findall("tr") if tbody is not None else []
    data_rows = [
        ["".join(cell.itertext()).strip() for cell in tr.findall("td")][:max_columns]
        for tr in body_rows[:max_rows]
    ]

    return columns, data_rows


def _preview_from_materializations(context: RunStatusSensorContext) -> tuple[str, list[str], list[list[str]]]:
    """Find the first materialized asset reporting a "preview" metadata entry and parse it.

    Mirrors whatever MaterializeResult.metadata={"preview": MetadataValue.md(...)}
    the asset itself chose to report -- e.g. load_meta_series_to_s3's CSV head()
    or ingest_bloomberg_values' fetched values -- rather than the sensor
    re-deriving its own preview. Returns ("", [], []) if no materialized asset
    in this run reported one.
    """
    records = context.instance.get_records_for_run(
        run_id=context.dagster_run.run_id,
        of_type=DagsterEventType.ASSET_MATERIALIZATION,
    ).records

    for record in records:
        materialization = record.event_log_entry.dagster_event.event_specific_data.materialization
        preview_value = materialization.metadata.get("preview")
        if preview_value is None:
            continue
        columns, rows = _parse_markdown_table(str(preview_value.value))
        if columns:
            return materialization.asset_key.to_user_string(), columns, rows

    return "", [], []


def _materialized_assets(context: RunStatusSensorContext, *, all_checks_passed: bool) -> list[dict[str, Any]]:
    """One row per materialized asset, summarizing whatever MaterializeResult metadata it reported.

    Our real assets don't report a "delta vs last run" or "size" the way the
    original mockup's fictional job did -- those columns are left blank
    rather than invented. status is a run-wide flag (not per-asset check
    matching) since that's what's cheaply and honestly derivable here.
    """
    records = context.instance.get_records_for_run(
        run_id=context.dagster_run.run_id,
        of_type=DagsterEventType.ASSET_MATERIALIZATION,
    ).records

    rows = []
    for record in records:
        materialization = record.event_log_entry.dagster_event.event_specific_data.materialization
        flat_metadata = {key: value.value for key, value in materialization.metadata.items()}
        summary_parts = [
            f"{key}={value}"
            for key, value in flat_metadata.items()
            if key != "preview" and not isinstance(value, (list, dict))
        ]
        rows.append(
            {
                "name": materialization.asset_key.to_user_string(),
                "rows": ", ".join(summary_parts) or "materialized",
                "delta": None,
                "delta_up": None,
                "size": "",
                "status": "OK" if all_checks_passed else "WARN",
            }
        )
    return rows


def _check_results(context: RunStatusSensorContext) -> list[dict[str, Any]]:
    """One row per asset check evaluated in the run."""
    records = context.instance.get_records_for_run(
        run_id=context.dagster_run.run_id,
        of_type=DagsterEventType.ASSET_CHECK_EVALUATION,
    ).records

    rows = []
    for record in records:
        evaluation = record.event_log_entry.dagster_event.event_specific_data
        rows.append(
            {
                "label": f"{evaluation.asset_key.to_user_string()}: {evaluation.check_name}",
                "status": "PASS" if evaluation.passed else evaluation.severity.value,
                "note": None if evaluation.passed else evaluation.description,
            }
        )
    return rows


def _send(email: OutlookEmailResource, *, subject: str, html_body: str) -> None:
    email.send_email(
        subject=subject,
        body="This notification requires an HTML-capable email client to view.",
        html_body=html_body,
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.STOPPED,
)
def run_succeeded_email_sensor(context: RunStatusSensorContext) -> None:
    """Email run_succeeded.html, or run_warning.html if any asset check didn't pass.

    STOPPED by default -- OutlookEmailResource has no real SMTP mailbox
    configured yet; flip default_status to RUNNING (or start it from the
    Dagster UI) once real credentials are wired into the "email" resource.
    """
    email: OutlookEmailResource = context.resources.email
    run = context.dagster_run
    stats = context.instance.get_run_stats(run.run_id)

    checks = _check_results(context)
    all_passed = all(check["status"] == "PASS" for check in checks)
    preview_name, preview_columns, preview_rows = _preview_from_materializations(context)

    common: dict[str, Any] = dict(
        job_name=run.job_name,
        partition=run.tags.get("dagster/partition", "—"),
        run_id=run.run_id,
        run_url=_run_url(run.run_id),
        source_system=run.job_name,
        fields_fetched="—",
        triggered_by=_triggered_by(run.tags),
        start_end=f"{_format_utc(stats.start_time)} → {_format_utc(stats.end_time)}",
        duration=_format_duration(stats.start_time, stats.end_time),
        environment=config("DAGSTER_ENVIRONMENT", default="development"),
        assets=_materialized_assets(context, all_checks_passed=all_passed),
        preview_table_name=preview_name or run.job_name,
        preview_columns=preview_columns,
        preview_rows=preview_rows,
    )

    if all_passed:
        subject = f"Run Succeeded - {run.job_name}"
        html = _render(
            "run_succeeded.html",
            subject=subject,
            preheader=f"{run.job_name} succeeded.",
            checks=checks or [{"label": "No asset checks configured for this run", "status": "PASS"}],
            **common,
        )
    else:
        subject = f"Run Completed with Warnings - {run.job_name}"
        html = _render(
            "run_warning.html",
            subject=subject,
            preheader=f"{run.job_name} completed with {sum(1 for c in checks if c['status'] != 'PASS')} check warning(s).",
            checks=checks,
            **common,
        )

    _send(email, subject=subject, html_body=html)


@run_failure_sensor(
    default_status=DefaultSensorStatus.STOPPED,
)
def run_failed_email_sensor(context: RunFailureSensorContext) -> None:
    """Email run_failed.html with the real failing step/exception.

    STOPPED by default -- see run_succeeded_email_sensor's note.
    """
    email: OutlookEmailResource = context.resources.email
    run = context.dagster_run
    stats = context.instance.get_run_stats(run.run_id)

    failure_events = context.get_step_failure_events()
    if failure_events:
        error = failure_events[0].event_specific_data.error
        root = error.cause or error  # unwrap Dagster's step-execution wrapper
        failed_step = failure_events[0].step_key or "unknown step"
        error_class = root.cls_name or "Exception"
        error_message = root.message or ""
        stack_trace = "".join(root.stack) if root.stack else root.to_string()
    else:
        failed_step = "unknown step"
        error_class = "Unknown error"
        error_message = "No step failure event was recorded for this run."
        stack_trace = ""

    run_url = _run_url(run.run_id)
    subject = f"Run Failed - {run.job_name}"

    html = _render(
        "run_failed.html",
        subject=subject,
        preheader=f"{failed_step} failed: {error_class}",
        job_name=run.job_name,
        partition=run.tags.get("dagster/partition", "—"),
        run_id=run.run_id,
        run_url=run_url,
        source_system=run.job_name,
        failed_step=failed_step,
        attempt="1 of 1 (automatic retries are not configured for this job)",
        triggered_by=_triggered_by(run.tags),
        failure_time=_format_utc(stats.end_time),
        environment=config("DAGSTER_ENVIRONMENT", default="development"),
        error_class=error_class,
        error_message=error_message,
        stack_trace=stack_trace,
        # Our jobs don't track a downstream dependency graph the way the
        # original mockup's fictional job did -- left empty rather than invented.
        downstream=[],
        next_steps=[
            "Open the run's logs in the Dagster UI for the full traceback and step context.",
            "Re-run the job once the underlying issue is resolved.",
        ],
        logs_url=run_url,
        runbook_url=config("RUNBOOK_URL", default=run_url),
        oncall_email=config("ONCALL_EMAIL", default="oncall@example.com"),
    )

    _send(email, subject=subject, html_body=html)
