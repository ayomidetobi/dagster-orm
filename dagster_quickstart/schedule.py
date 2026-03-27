"""Dagster schedule definitions.

Defines schedules for running jobs at specific times.
"""

from dagster import ScheduleDefinition

from dagster_quickstart.jobs import populate_value_data_job

# Schedule: Bloomberg daily ingestion only. Derived series use ``calculate_derived_series_job``
# (partitioned by SPREAD, FLY, BOX, RATIO, SPREAD_INV, RATIO_INV); run separately or via backfill.
#
# Cron: every weekday at 9 AM Lisbon time
# Cron expression: "0 9 * * 1-5" means:
# - 0: minute (0th minute)
# - 9: hour (9 AM)
# - *: day of month (any)
# - *: month (any)
# - 1-5: day of week (Monday=1 through Friday=5)
populate_value_data_schedule = ScheduleDefinition(
    job=populate_value_data_job,
    name="populate_value_data_schedule",
    cron_schedule="0 9 * * 1-5",  # 9 AM Monday-Friday
    execution_timezone="Europe/Lisbon",
)
