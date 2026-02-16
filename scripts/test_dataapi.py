#!/usr/bin/env python3
"""Simple script template to read metadata info and value data using DataAPI.

Usage:
    python scripts/test_dataapi.py
    # Or with environment variables:
    S3_BUCKET=my-bucket S3_ACCESS_KEY=xxx S3_SECRET_KEY=xxx python scripts/test_dataapi.py
"""

import sys
from pathlib import Path

# Add parent directory to path to import dagster_quickstart
sys.path.insert(0, str(Path(__file__).parent.parent))

from decouple import config

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource

# Initialize DuckDB resource
duckdb_cacher = duckdb_datacacher(
    bucket=config("S3_BUCKET", default=None),
    access_key=config("S3_ACCESS_KEY", default=None),
    secret_key=config("S3_SECRET_KEY", default=None),
    region=config("S3_REGION", default=None),
)

duckdb_resource = DuckDBResource(cacher=duckdb_cacher)
duckdb_resource.setup_for_execution(None)

# Create DataAPI instance
data_api = DataAPI(duckdb_resource)

# Example 1: Query metadata with filters
print("=" * 60)
print("Example 1: Query metadata")
print("=" * 60)
dataset = data_api.get(
    series_code=["TSLA_PX_LAST"],
)
metadata_df = dataset.info()
print(f"Found {len(metadata_df)} metadata rows")
print(f"Columns: {', '.join(metadata_df.columns)}")
if not metadata_df.empty:
    print(f"\nFirst row:\n{metadata_df.iloc[0]}")

# Example 2: Get value data for the filtered series
print("\n" + "=" * 60)
print("Example 2: Get value data")
print("=" * 60)
values_df = dataset.value(
    ValueQueryParams(
        start="2025-02-01",
        end="2026-02-16",
    )
)
print(values_df.head(10))
if not values_df.empty:
    print(f"Columns: {', '.join(values_df.columns)}")
    print(f"Date range: {values_df['timestamp'].min()} to {values_df['timestamp'].max()}")
