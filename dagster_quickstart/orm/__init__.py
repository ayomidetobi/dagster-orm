"""Semantic ORM layer for DuckDB with S3 datalake.

This package provides a high-level ORM interface built on top of DuckDB Tiny ORM
for querying metadata and value data from S3 Parquet files.

Main entry point:
    from dagster_quickstart.orm import DataAPI
    from dagster_quickstart.orm.query_params import ValueQueryParams

    # In a Dagster asset:
    duckdb_resource = context.resources.duckdb
    data_api = DataAPI(duckdb_resource)
    dataset = data_api.get(asset_class=["fx"], country=["usa"])
    metadata = dataset.info()
    values = dataset.value(ValueQueryParams(start="2024-01-01"))
"""

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.s3_paths import (
    build_s3_control_table_path,
    build_s3_value_data_path,
)
from dagster_quickstart.orm.validation import MetadataValidator

__all__ = [
    "DataAPI",
    "ValueQueryParams",
    "MetadataValidator",
    "build_s3_control_table_path",
    "build_s3_value_data_path",
]
