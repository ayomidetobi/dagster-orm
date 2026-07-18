"""Dagster schedule definitions.

Defines schedules for running jobs at specific times.
"""

from dagster import ScheduleDefinition

from dagster_quickstart.jobs import (
    bloomberg_daily_ingestion_job,
    hawk_daily_ingestion_job,
)

# Cron: every weekday at 9 AM Lisbon time
# Cron expression: "0 9 * * 1-5" means:
# - 0: minute (0th minute)
# - 9: hour (9 AM)
# - *: day of month (any)
# - *: month (any)
# - 1-5: day of week (Monday to Friday)
_DAILY_9AM_CRON = "0 9 * * 1-5"
_DAILY_9AM_TIMEZONE = "Europe/Lisbon"

bloomberg_daily_schedule = ScheduleDefinition(
    job=bloomberg_daily_ingestion_job,
    name="bloomberg_daily_schedule",
    cron_schedule=_DAILY_9AM_CRON,
    execution_timezone=_DAILY_9AM_TIMEZONE,
)

hawk_daily_schedule = ScheduleDefinition(
    job=hawk_daily_ingestion_job,
    name="hawk_daily_schedule",
    cron_schedule=_DAILY_9AM_CRON,
    execution_timezone=_DAILY_9AM_TIMEZONE,
)
