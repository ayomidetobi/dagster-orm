"""Dagster sensor definitions."""

# derived_after_ingestion_sensor pulls in dagster_quickstart.jobs, which
# references every asset -- commented out while assets/__init__.py stays
# trimmed to a subset for focused testing. Uncomment both together.
# from dagster_quickstart.sensors.derived_after_ingestion import derived_after_ingestion_sensor
from dagster_quickstart.sensors.job_chaining import build_run_after_job_sensor
from dagster_quickstart.sensors.run_notifications import (
    run_failed_email_sensor,
    run_succeeded_email_sensor,
)

__all__ = [
    # "derived_after_ingestion_sensor",
    "build_run_after_job_sensor",
    "run_failed_email_sensor",
    "run_succeeded_email_sensor",
]
