from dagster import Definitions
from decouple import config

from dagster_quickstart.assets import (
    calculate_derived_series,
    ingest_bloomberg_data_backfill,
    ingest_bloomberg_data_daily,
    ingest_hawk_data_backfill,
    ingest_hawk_data_daily,
    load_lookup_tables_to_s3,
    load_meta_series_to_s3,
    load_series_dependencies_to_s3,
    validate_metadata_against_lookup,
    validate_parent_series_count,
)
from dagster_quickstart.jobs import (
    all_assets_job,
    bloomberg_backfill_ingestion_job,
    bloomberg_daily_ingestion_job,
    calculate_derived_series_job,
    hawk_backfill_ingestion_job,
    hawk_daily_ingestion_job,
    load_control_tables_job,
    populate_value_data_job,
)
from dagster_quickstart.orm.io_manager import duckdb_io_manager
from dagster_quickstart.schedule import populate_value_data_schedule
from dagster_quickstart.resources import DuckDBResource, HawkResource, PyPDLResource
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher

all_assets = [
    load_lookup_tables_to_s3,
    load_meta_series_to_s3,
    load_series_dependencies_to_s3,
    ingest_bloomberg_data_daily,
    ingest_bloomberg_data_backfill,
    ingest_hawk_data_daily,
    ingest_hawk_data_backfill,
    calculate_derived_series,
]

all_asset_checks = [
    validate_metadata_against_lookup,
    validate_parent_series_count,
]

# Initialize DuckDB datacacher with S3 credentials from environment
# You can configure these via environment variables or pass directly
duckdb_cacher = duckdb_datacacher(
    bucket=config("S3_BUCKET", default=None),
    access_key=config("S3_ACCESS_KEY", default=None),
    secret_key=config("S3_SECRET_KEY", default=None),
    region=config("S3_REGION", default=None),
)

# Initialize DuckDB resource with datacacher
duckdb_resource = DuckDBResource(cacher=duckdb_cacher)

# Initialize PyPDL resource
pypdl_resource = PyPDLResource(
    host=config("PYPDL_HOST", default="localhost"),
    port=config("PYPDL_PORT", default=8194, cast=int),
    username=config("PYPDL_USERNAME", default=""),
)

# Demo Hawk (MQL) resource — optional env override for broker URL
hawk_resource = HawkResource(
    celery_connection=config("HAWK_CELERY_CONNECTION", default="demo://localhost"),
)

# Define resources
resources = {
    "duckdb": duckdb_resource,
    "hawk": hawk_resource,
    "pypdl": pypdl_resource,
    "io_manager": duckdb_io_manager,
    "duckdb_io_manager": duckdb_io_manager,
}

all_jobs = [
    load_control_tables_job,
    bloomberg_daily_ingestion_job,
    bloomberg_backfill_ingestion_job,
    hawk_daily_ingestion_job,
    hawk_backfill_ingestion_job,
    calculate_derived_series_job,
    populate_value_data_job,
    # all_assets_job,
]

all_schedules = [
    populate_value_data_schedule,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=resources,
)
