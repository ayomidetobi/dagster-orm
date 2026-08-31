"""Daily STEER summary email -- new signals, cointegration failures, sign-drops.

Deliberately NOT a run_status_sensor like run_notifications.py's: the
digest needs to summarize *every pair's* result for the day, but
steer_daily_schedule launches one Dagster run per pair (see
assets/steer/job.py), so there's no single "the run" to react to. Instead
this reads directly from gold.steer_estimates/gold.steer_signals (today's
rows, across every pair) -- the same data the per-pair runs just wrote --
and reuses the existing run_succeeded.html/run_warning.html templates
(mapping each pair onto the templates' existing assets/checks/preview
placeholders) rather than introducing a new template, per
"extend the existing template system".
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from dagster import DefaultScheduleStatus, ScheduleEvaluationContext, schedule
from decouple import config

from dagster_quickstart.resources.outlook_email_resource import OutlookEmailResource
from dagster_quickstart.sensors.email_helpers import render
from dagster_quickstart.steer.storage import (
    GOLD_SCHEMA,
    STEER_ESTIMATES_TABLE,
    STEER_SIGNALS_TABLE,
    SteerCatalog,
)

# Fires after steer_daily_schedule's per-pair runs (09:00 Europe/Lisbon,
# see assets/steer/job.py) have had time to complete.
_DIGEST_CRON = "30 9 * * 1-5"
_DIGEST_TIMEZONE = "Europe/Lisbon"


def _todays_rows(
    catalog: SteerCatalog, schema: str, table: str, *, today: pd.Timestamp
) -> pd.DataFrame:
    frame = catalog.read(schema, table)
    if frame.empty:
        return frame
    return frame[pd.to_datetime(frame["date"]).dt.normalize() == today.normalize()]


def _assets_rows(signals: pd.DataFrame) -> list[dict[str, Any]]:
    """One row per pair, in the run_succeeded.html "assets" table shape."""
    rows = []
    for _, row in signals.iterrows():
        rows.append(
            {
                "name": f"{row['universe']}/{row['currency_pair']}",
                "rows": f"signal={row['signal']}, z={row['entry_z_score']:.2f}, {row['reason']}",
                "delta": None,
                "delta_up": None,
                "size": "",
                "status": "OK" if row["signal"] != "NONE" else "—",
            }
        )
    return rows


def _check_rows(estimates: pd.DataFrame) -> list[dict[str, Any]]:
    """One row per pair's cointegration test + sign-drop outcome, in the templates' "checks" shape."""
    rows = []
    for _, row in estimates.iterrows():
        pair = f"{row['universe']}/{row['currency_pair']}"
        rows.append(
            {
                "label": f"{pair}: cointegration",
                "status": "PASS" if row["cointegration_passed"] else "FAIL",
                "note": None if row["cointegration_passed"] else "Engle-Granger test did not pass",
            }
        )
        if row["sign_dropped"]:
            rows.append(
                {
                    "label": f"{pair}: sign check",
                    "status": "WARN",
                    "note": f"Dropped variable(s) with the wrong sign: {row['dropped_variables']}",
                }
            )
    return rows


@schedule(
    cron_schedule=_DIGEST_CRON,
    execution_timezone=_DIGEST_TIMEZONE,
    job_name="steer_daily_run",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"steer_catalog", "email"},
)
def steer_daily_digest_schedule(context: ScheduleEvaluationContext) -> None:
    """Build and send the daily STEER digest. STOPPED by default -- see run_notifications.py's note on OutlookEmailResource."""
    catalog: SteerCatalog = context.resources.steer_catalog.catalog
    email: OutlookEmailResource = context.resources.email

    today = pd.Timestamp(context.scheduled_execution_time).normalize()
    estimates = _todays_rows(catalog, GOLD_SCHEMA, STEER_ESTIMATES_TABLE, today=today)
    signals = _todays_rows(catalog, GOLD_SCHEMA, STEER_SIGNALS_TABLE, today=today)

    if estimates.empty and signals.empty:
        context.log.info("No STEER estimates/signals for today yet -- skipping digest.")
        return

    cointegration_failures = (
        int((~estimates["cointegration_passed"]).sum()) if not estimates.empty else 0
    )
    sign_drops = int(estimates["sign_dropped"].sum()) if not estimates.empty else 0
    new_signals = int((signals["signal"] != "NONE").sum()) if not signals.empty else 0

    checks = _check_rows(estimates)
    all_passed = cointegration_failures == 0

    common: dict[str, Any] = dict(
        job_name="steer_daily_run",
        partition="—",
        run_id="—",
        run_url=config("DAGSTER_WEBSERVER_URL", default="http://localhost:3000"),
        source_system="steer_daily_run",
        fields_fetched="—",
        triggered_by=f"Schedule · {context.schedule_name}",
        start_end=f"{today.date()}",
        duration="—",
        environment=config("DAGSTER_ENVIRONMENT", default="development"),
        assets=_assets_rows(signals),
        preview_table_name="steer_signals",
        preview_columns=list(signals.columns) if not signals.empty else [],
        preview_rows=signals.astype(str).values.tolist() if not signals.empty else [],
    )

    if all_passed:
        subject = f"STEER Daily Digest - {new_signals} new signal(s)"
        html = render(
            "run_succeeded.html",
            subject=subject,
            preheader=f"{new_signals} new signal(s), {sign_drops} sign-drop(s), 0 cointegration failures.",
            checks=checks or [{"label": "No cointegration tests ran today", "status": "PASS"}],
            **common,
        )
    else:
        subject = f"STEER Daily Digest - {cointegration_failures} cointegration failure(s)"
        html = render(
            "run_warning.html",
            subject=subject,
            preheader=(
                f"{new_signals} new signal(s), {sign_drops} sign-drop(s), "
                f"{cointegration_failures} cointegration failure(s)."
            ),
            checks=checks,
            **common,
        )

    email.send_email(
        subject=subject,
        body="This notification requires an HTML-capable email client to view.",
        html_body=html,
    )
