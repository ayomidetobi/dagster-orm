"""Pydantic configuration models for DuckDB/DuckLake connections.

Moved here from rewrite/data_api/models/config.py -- these models are
consumed by ducklake_datacacher.py (DuckLakeCatalogConfig, DuckLakeConfig,
S3SecretConfig) and have no dependency on the rewrite package themselves,
so they live alongside the connection code that actually uses them.
rewrite/data_api/models/config.py re-exports from here unchanged.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PostgresConfig(BaseModel):
    """Connection settings for PostgreSQL-backed DuckLake catalogs."""

    model_config = ConfigDict(frozen=True)

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "public"
    sslmode: str | None = None


class DuckLakeConfig(BaseModel):
    """Configuration for DuckLake extension installation."""

    model_config = ConfigDict(frozen=True)

    extension_name: str = "ducklake"
    postgres_extension_name: str = "postgres"
    httpfs_extension_name: str = "httpfs"
    install: bool = True
    load: bool = True


class S3SecretConfig(BaseModel):
    """DuckDB secret configuration for S3 access via httpfs."""

    model_config = ConfigDict(frozen=True)

    name: str = "secret"
    key_id: str
    secret: str
    region: str
    provider: str = "config"
    session_token: str | None = None
    endpoint: str | None = None


class DuckLakeCatalogConfig(BaseModel):
    """DuckLake attach configuration for PostgreSQL and object storage."""

    model_config = ConfigDict(frozen=True)

    postgres: PostgresConfig | None = None
    s3_secret: S3SecretConfig | None = None
    catalog_alias: str = "my_ducklake"
    data_path: str = "data_files/"
    attach_options: tuple[str, ...] = ()
    schema_name: str = "public"
