"""Backwards-compatible re-export shim.

The actual implementation now lives in resources/duckdb_cacher/, split by
concern (utils/config/exceptions/duckdb_datacacher/ducklake_datacacher) --
see that package's __init__.py for details. Every name that used to be
importable from this module still is; new code should prefer importing
directly from resources.duckdb_cacher (or its submodules).
"""

from __future__ import annotations

from .duckdb_cacher import (
    SQL,
    DuckDBConnectionFactory,
    DuckDBDataCacher,
    DuckLakeCatalogBackend,
    DuckLakeCatalogConfig,
    DuckLakeConfig,
    DuckLakeConfigError,
    PostgresConfig,
    PostgresDuckLakeCatalogBackend,
    S3SecretConfig,
    SQLIdentifier,
    collect_dataframes,
    create_duckdb_connection,
    duckdb_datacacher,
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
    "create_duckdb_connection",
    "DuckDBDataCacher",
    "duckdb_datacacher",
    "PostgresConfig",
    "DuckLakeConfig",
    "S3SecretConfig",
    "DuckLakeCatalogConfig",
    "DuckLakeConfigError",
]
