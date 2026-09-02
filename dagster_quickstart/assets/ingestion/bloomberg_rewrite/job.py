"""bloomberg_values_daily_run: keeps ingest_bloomberg_values' bronze data fresh.

Without a schedule, ingest_bloomberg_values only ever runs when someone
materializes it by hand -- the demo vendor client (see
rewrite/data_api/vendors/demo_data.py) only "has" data for whatever window
it was last asked to fetch, so a series with no recent manual run has zero
rows in the values table, not just stale ones. That's a silent prerequisite
STEER's freshness check depends on (see assets/steer/silver_asset.py) but
can't itself satisfy -- steer_daily_schedule only ever reads bronze data,
it never refreshes it.

Runs before steer_daily_schedule (08:00 vs. 09:00 Europe/Lisbon) so the
same day's STEER run reads same-day bronze data.
"""

from dagster import RunRequest, ScheduleEvaluationContext, define_asset_job, schedule

from dagster_quickstart.assets.ingestion.bloomberg_rewrite import ingest_bloomberg_values

bloomberg_values_daily_job = define_asset_job(
    name="bloomberg_values_daily_run",
    selection=[ingest_bloomberg_values],
)

_DAILY_8AM_CRON = "0 8 * * 1-5"
_DAILY_8AM_TIMEZONE = "Europe/Lisbon"


@schedule(
    job=bloomberg_values_daily_job,
    cron_schedule=_DAILY_8AM_CRON,
    execution_timezone=_DAILY_8AM_TIMEZONE,
)
def bloomberg_values_daily_schedule(context: ScheduleEvaluationContext):
    """Refresh every Bloomberg-tickered series' bronze values, once a day."""
    yield RunRequest(run_key=f"bloomberg_values_daily-{context.scheduled_execution_time.date()}")
