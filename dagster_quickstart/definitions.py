from dagster import Definitions
from decouple import Csv, config

from dagster_quickstart.assets import (
    ingest_bloomberg_values,
    load_meta_series_to_s3,
    steer_assets,
)
from dagster_quickstart.assets.steer.job import steer_daily_job, steer_daily_schedule
from dagster_quickstart.resources import (
    HawkResource,
    OutlookEmailResource,
    RewriteDataAPIResource,
    SteerCatalogResource,
    SteerConfigResource,
)
from dagster_quickstart.sensors import (
    run_failed_email_sensor,
    run_succeeded_email_sensor,
    steer_daily_digest_schedule,
)

all_assets = [
    load_meta_series_to_s3,
    ingest_bloomberg_values,
    *steer_assets,
]

all_asset_checks = [
    # validate_metadata_quality now runs in-asset via load_meta_series_to_s3's
    # check_specs -- no separate object to list here. Same for the STEER
    # freshness/features/estimates/signals/data_availability checks -- see
    # assets/steer/*.py's check_specs.
]

all_jobs = [
    steer_daily_job,
]

# DuckLake-backed DataAPI (rewrite/data_api/) -- zero-config, reads
# DATABASE_URL/S3_* straight from the environment.
rewrite_data_api_resource = RewriteDataAPIResource(
    live=config("REWRITE_DATA_API_LIVE", default=False, cast=bool),
)

# Demo Hawk (MQL) resource — optional env override for broker URL
hawk_resource = HawkResource(
    celery_connection=config("HAWK_CELERY_CONNECTION", default="demo://localhost"),
)

# Demo email resource -- no real mailbox configured yet, just placeholder
# defaults so Definitions loads cleanly. Set OUTLOOK_EMAIL_* env vars to a
# real account before turning on run_succeeded_email_sensor/
# run_failed_email_sensor/steer_daily_digest_schedule (all start STOPPED --
# see sensors/run_notifications.py, sensors/steer_notifications.py).
outlook_email_resource = OutlookEmailResource(
    email_from=config("OUTLOOK_EMAIL_FROM", default="dagster-notifications@example.com"),
    email_password=config("OUTLOOK_EMAIL_PASSWORD", default="demo-password-not-set"),
    email_to=config("OUTLOOK_EMAIL_TO", default="oncall@example.com", cast=Csv()),
)

# StrategyConfig (G10/EM/CHN) loaded + validated once at process start from
# steer/strategy_configs/*.yaml -- see steer/config.py.
steer_config_resource = SteerConfigResource()

# STEER gold/silver DuckLake schemas -- see steer/storage.py.
steer_catalog_resource = SteerCatalogResource()

resources = {
    "rewrite_data_api": rewrite_data_api_resource,
    "hawk": hawk_resource,
    "email": outlook_email_resource,
    "steer_config": steer_config_resource,
    "steer_catalog": steer_catalog_resource,
}

all_schedules = [
    steer_daily_schedule,
    steer_daily_digest_schedule,
]

all_sensors = [
    run_succeeded_email_sensor,
    run_failed_email_sensor,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources=resources,
)
