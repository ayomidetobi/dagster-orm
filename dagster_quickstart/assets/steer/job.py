"""steer_daily_run: the job + schedule that materialize every STEER partition daily.

Job/partition design notes:
  - STEER_PARTITIONS is a single static dimension -- variant (G10/EM/CHN,
    see partitions.py's module docstring). currency_pair is NOT a Dagster
    partition; each variant's run fetches and processes every pair in
    that variant as data (see steer/discovery.py's discover_pairs()).
  - Because the partition set is static and small, the schedule needs no
    live datalake query and no partition-registration step (an earlier
    per-pair-partition design needed steer_pair_discovery_sensor for
    exactly that -- removed along with the dynamic partition it served).
  - Each asset recomputes "as of today" (or config.as_of, for a backfill)
    every time it's materialized, the same way ingest_bloomberg_values
    re-fetches everything fresh on every run rather than being
    date-partitioned.
  - One variant partition's asset check failing (e.g. steer_silver_prices'
    freshness check reporting some stale pairs) fails *that partition's*
    run only -- Dagster's per-partition run isolation means the other
    variants' runs are unaffected. Within a partition, one pair's data
    gap or cointegration failure never fails the whole run either -- see
    each asset's own per-pair try/except and steer/signals.py's NONE
    signal for a failed cointegration test.
"""

from dagster import RunRequest, ScheduleEvaluationContext, define_asset_job, schedule

from dagster_quickstart.assets.steer import steer_assets
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS

steer_daily_job = define_asset_job(
    name="steer_daily_run",
    selection=steer_assets,
    partitions_def=STEER_PARTITIONS,
)

# Matches the repo's own daily-ingestion schedule convention (see git
# history for the now-superseded bloomberg_daily_schedule/
# hawk_daily_schedule in the legacy orm-based schedule.py): 09:00
# Europe/Lisbon, weekdays only -- not the 09:00 UTC default the brief
# suggested falling back to, since a real prior convention exists.
_DAILY_9AM_CRON = "0 9 * * 1-5"
_DAILY_9AM_TIMEZONE = "Europe/Lisbon"


@schedule(
    job=steer_daily_job,
    cron_schedule=_DAILY_9AM_CRON,
    execution_timezone=_DAILY_9AM_TIMEZONE,
)
def steer_daily_schedule(context: ScheduleEvaluationContext):
    """Launch one run per variant partition (G10, EM, CHN) -- see job.py's module docstring."""
    for variant in STEER_PARTITIONS.get_partition_keys():
        yield RunRequest(
            run_key=f"steer_daily-{context.scheduled_execution_time.date()}-{variant}",
            partition_key=variant,
        )
