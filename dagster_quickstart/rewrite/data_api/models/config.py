"""Backwards-compatible re-export shim.

These Pydantic models now live in resources/duckdb_cacher/config.py --
moved there since they're consumed by the DuckDB/DuckLake connection layer,
not anything specific to the rewrite package. Re-exported here unchanged so
existing `from dagster_quickstart.rewrite.data_api.models.config import ...` imports keep working.
"""

from __future__ import annotations

from dagster_quickstart.resources.duckdb_cacher.config import (
    DuckLakeCatalogConfig,
    DuckLakeConfig,
    PostgresConfig,
    S3SecretConfig,
)

__all__ = [
    "DuckLakeCatalogConfig",
    "DuckLakeConfig",
    "PostgresConfig",
    "S3SecretConfig",
]
