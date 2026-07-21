"""DuckDB/DuckLake connection and caching utilities.

Split by concern:
- utils.py: shared SQL-rendering primitives (SQL, render_ducklake_sql, etc.)
- config.py: Pydantic connection config models
- exceptions.py: DuckLakeConfigError
- duckdb_datacacher.py: legacy S3-Parquet cache (DuckDBDataCacher), no DuckLake awareness
- ducklake_datacacher.py: DuckLake catalog attach/connection factory

This package is the canonical home; resources/duckdb_datacacher.py (the
old single-file module) re-exports everything from here unchanged, so
existing `from resources.duckdb_datacacher import ...` imports keep working.
"""

from __future__ import annotations

from .config import (
    DuckLakeCatalogConfig,
    DuckLakeConfig,
    PostgresConfig,
    S3SecretConfig,
)
from .ducklake_datacacher import (
    DuckDBConnectionFactory,
    DuckLakeCatalogBackend,
    DuckLakeDataCacher,
    PostgresDuckLakeCatalogBackend,
    create_duckdb_connection,
)
from .duckdb_datacacher import DuckDBDataCacher, duckdb_datacacher
from .exceptions import DuckLakeConfigError
from .utils import (
    SQL,
    SQLIdentifier,
    collect_dataframes,
    install_plugin,
    join_s3,
    quote_identifier,
    render_ducklake_sql,
    sql_literal,
)

__all__ = [
    "SQL",
    "SQLIdentifier",
    "quote_identifier",
    "sql_literal",
    "render_ducklake_sql",
    "join_s3",
    "collect_dataframes",
    "install_plugin",
    "DuckLakeCatalogBackend",
    "PostgresDuckLakeCatalogBackend",
    "DuckDBConnectionFactory",
    "DuckLakeDataCacher",
    "create_duckdb_connection",
    "DuckDBDataCacher",
    "duckdb_datacacher",
    "PostgresConfig",
    "DuckLakeConfig",
    "S3SecretConfig",
    "DuckLakeCatalogConfig",
    "DuckLakeConfigError",
]
