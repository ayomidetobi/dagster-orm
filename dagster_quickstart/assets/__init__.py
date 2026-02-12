"""Assets package for loading control tables to S3 as Parquet files.

Uses ORM layer (DataAPI) for all operations - no raw SQL in asset code.
All temp table management, queries, and S3 operations go through the ORM.
"""

from dagster_quickstart.assets.load_lookup import load_lookup_tables_to_s3
from dagster_quickstart.assets.load_metaseries import load_meta_series_to_s3

__all__ = [
    "load_lookup_tables_to_s3",
    "load_meta_series_to_s3",
]
