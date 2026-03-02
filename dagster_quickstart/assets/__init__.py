"""Assets package for loading control tables to S3 as Parquet files.

Uses ORM layer (DataAPI) for all operations - no raw SQL in asset code.
All temp table management, queries, and S3 operations go through the ORM.
"""

from dagster_quickstart.assets.derived import calculate_derived_series
from dagster_quickstart.assets.ingestion.bloomberg import (
    ingest_bloomberg_data_backfill,
    ingest_bloomberg_data_daily,
)
from dagster_quickstart.assets.load_lookup import load_lookup_tables_to_s3
from dagster_quickstart.assets.load_metaseries import (
    load_meta_series_to_s3,
    validate_metadata_against_lookup,
)
from dagster_quickstart.assets.load_series_dependencies import (
    load_series_dependencies_to_s3,
    validate_parent_series_count,
)

__all__ = [
    "calculate_derived_series",
    "load_lookup_tables_to_s3",
    "load_meta_series_to_s3",
    "load_series_dependencies_to_s3",
    "ingest_bloomberg_data_daily",
    "ingest_bloomberg_data_backfill",
    "validate_metadata_against_lookup",
    "validate_parent_series_count",
]
