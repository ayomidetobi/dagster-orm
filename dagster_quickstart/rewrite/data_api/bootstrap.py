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

from resources.duckdb_datacacher import create_duckdb_connection
from rewrite.data_api.columns import TickerSource
from rewrite.data_api.container import build_rewrite_container
from rewrite.data_api.models.config import DuckLakeCatalogConfig, PostgresConfig, S3SecretConfig
from rewrite.data_api.repositories.ducklake_meta_repository import (
    DuckLakeMetadataStorageRepository,
)
from rewrite.data_api.repositories.ducklake_value_repository import (
    DuckLakeValueStorageRepository,
)
from rewrite.data_api.services.vendor_service import VendorClient
from rewrite.data_api.vendors.bloomberg import BloombergClient
from rewrite.data_api.vendors.hawk import HawkClient
from rewrite.data_api.vendors.mds import MDSClient

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


def build_default_container(
    *,
    vendor_clients: Mapping[str, VendorClient] | None = None,
):
    """Build a RewriteContainer attached to the real Postgres+S3 DuckLake catalog.

    Reads DATABASE_URL, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION
    from the environment/.env via python-decouple.
    """

    bucket = env_config("S3_BUCKET")
    access_key = env_config("S3_ACCESS_KEY")
    secret_key = env_config("S3_SECRET_KEY")
    region = env_config("S3_REGION")
    postgres_config = _parse_database_url(env_config("DATABASE_URL"))

    catalog_config = DuckLakeCatalogConfig(
        postgres=postgres_config,
        s3_secret=S3SecretConfig(key_id=access_key, secret=secret_key, region=region),
        catalog_alias=DEFAULT_CATALOG_ALIAS,
        data_path=f"s3://{bucket}/{DEFAULT_DATA_PREFIX}",
        schema_name=DEFAULT_SCHEMA_NAME,
    )

    logger.info("data_api_bootstrap_connecting", host=postgres_config.host, bucket=bucket)
    connection = create_duckdb_connection(ducklake_catalog=catalog_config, enable_ducklake=True)

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
