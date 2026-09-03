"""Zero-config bootstrap: build a fully-wired RewriteServices straight from the environment.

Reads DATABASE_URL / S3_* from the environment (via python-decouple, which
also picks up a .env file) and attaches a real DuckLake catalog (Postgres
metadata catalog, S3-backed storage). This is what lets DataAPI() work with
no arguments -- see DataAPI.__init__ in api/data_api.py.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import parse_qs, urlparse

import structlog
from decouple import config as env_config

from dagster_quickstart.resources.duckdb_datacacher import create_duckdb_connection
from dagster_quickstart.rewrite.data_api.columns import TickerSource
from dagster_quickstart.rewrite.data_api.container import build_rewrite_container
from dagster_quickstart.rewrite.data_api.models.config import DuckLakeCatalogConfig, PostgresConfig, S3SecretConfig
from dagster_quickstart.rewrite.data_api.repositories.ducklake_meta_repository import (
    DuckLakeMetadataStorageRepository,
)
from dagster_quickstart.rewrite.data_api.repositories.ducklake_value_repository import (
    DuckLakeValueStorageRepository,
)
from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorClient
from dagster_quickstart.rewrite.data_api.vendors.bloomberg import BloombergClient
from dagster_quickstart.rewrite.data_api.vendors.hawk import HawkClient
from dagster_quickstart.rewrite.data_api.vendors.mds import MDSClient

logger = structlog.get_logger(__name__)

DEFAULT_CATALOG_ALIAS = "rewrite_ducklake_v2"
DEFAULT_DATA_PREFIX = "ducklake/"
DEFAULT_SCHEMA_NAME = "rewrite_ducklake_v2"


def _default_vendor_clients() -> dict[str, VendorClient]:
    """Vendor clients wired by default; pass vendor_clients=... to override/extend."""
    return {
        TickerSource.BLOOMBERG: BloombergClient(),
        TickerSource.HAWK: HawkClient(),
        TickerSource.MDS: MDSClient(),
    }


def _parse_database_url(url: str) -> PostgresConfig:
    """Parse a postgres:// connection string into a PostgresConfig."""

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return PostgresConfig(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
        sslmode=query.get("sslmode", [None])[0],
    )


def build_default_catalog_config() -> DuckLakeCatalogConfig:
    """Build the DuckLakeCatalogConfig for the real Postgres+S3 catalog from the environment.

    Reads DATABASE_URL, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION
    from the environment/.env via python-decouple. Shared by
    build_default_connection() (below) and anything else that needs to
    attach to this exact catalog without going through the full DataAPI
    stack.
    """

    bucket = env_config("S3_BUCKET")
    access_key = env_config("S3_ACCESS_KEY")
    secret_key = env_config("S3_SECRET_KEY")
    region = env_config("S3_REGION")
    postgres_config = _parse_database_url(env_config("DATABASE_URL"))

    return DuckLakeCatalogConfig(
        postgres=postgres_config,
        s3_secret=S3SecretConfig(key_id=access_key, secret=secret_key, region=region),
        catalog_alias=DEFAULT_CATALOG_ALIAS,
        data_path=f"s3://{bucket}/{DEFAULT_DATA_PREFIX}",
        schema_name=DEFAULT_SCHEMA_NAME,
    )


def build_default_connection():
    """Attach a fresh DuckDB connection to the real Postgres+S3 DuckLake catalog. No repository/schema setup.

    Just the ATTACH -- unlike build_default_container(), this never touches
    DuckLakeValueStorageRepository.initialize_schema() (schema-altering DDL
    on the `values` table). Two connections both running that DDL at once
    is a real DuckLake transaction conflict (optimistic concurrency control
    on the Postgres-backed catalog) -- callers that don't need the
    metadata/value repositories at all should use this instead of
    build_default_container() to avoid running that DDL redundantly and
    racing DataAPI's own initialization.
    """

    catalog_config = build_default_catalog_config()
    logger.info(
        "data_api_bootstrap_connecting", host=catalog_config.postgres.host, bucket=catalog_config.data_path
    )
    return create_duckdb_connection(ducklake_catalog=catalog_config, enable_ducklake=True)


def build_default_container(
    *,
    vendor_clients: Mapping[str, VendorClient] | None = None,
):
    """Build a RewriteContainer attached to the real Postgres+S3 DuckLake catalog.

    See build_default_connection()/build_default_catalog_config() for the
    connection-only path. This one also wires up the metadata/value
    repositories and runs DuckLakeValueStorageRepository.initialize_schema()
    (schema-altering DDL) -- appropriate for a real DataAPI, but two of
    these initializing concurrently against the same catalog is itself a
    real conflict risk; Dagster resources that build a DataAPI should be
    the only ones doing so per run.
    """

    connection = build_default_connection()

    metadata_repository = DuckLakeMetadataStorageRepository(connection)
    value_repository = DuckLakeValueStorageRepository(connection)
    value_repository.initialize_schema()

    return build_rewrite_container(
        duckdb_connection=connection,
        metadata_repository=metadata_repository,
        value_repository=value_repository,
        vendor_clients=dict(vendor_clients)
        if vendor_clients is not None
        else _default_vendor_clients(),
    )


def build_default_services(
    *,
    vendor_clients: Mapping[str, VendorClient] | None = None,
):
    """Build RewriteServices straight from the environment. See build_default_container()."""

    return build_default_container(vendor_clients=vendor_clients).services()
