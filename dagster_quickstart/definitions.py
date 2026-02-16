from dagster import Definitions
from decouple import config

from dagster_quickstart.assets import (
    load_lookup_tables_to_s3,
    load_meta_series_to_s3,
    load_series_dependencies_to_s3,
    validate_metadata_against_lookup,
    validate_parent_series_count,
)
from dagster_quickstart.orm.io_manager import duckdb_io_manager
from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher

all_assets = [
    load_lookup_tables_to_s3,
    load_meta_series_to_s3,
    load_series_dependencies_to_s3,
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

# Define resources
resources = {
    "duckdb": duckdb_resource,
    "io_manager": duckdb_io_manager,
    "duckdb_io_manager": duckdb_io_manager,
}

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources=resources,
)
