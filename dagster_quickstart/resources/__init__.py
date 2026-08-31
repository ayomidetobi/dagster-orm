"""Dagster resources for the financial platform."""

from dagster_quickstart.resources.duckdb_datacacher import (
    DuckDBConnectionFactory,
    DuckLakeCatalogBackend,
    PostgresDuckLakeCatalogBackend,
    create_duckdb_connection,
)
from dagster_quickstart.resources.hawk_resources import HawkResource
from dagster_quickstart.resources.outlook_email_resource import OutlookEmailResource
from dagster_quickstart.resources.rewrite_data_api_resource import RewriteDataAPIResource
from dagster_quickstart.resources.steer_catalog_resource import SteerCatalogResource
from dagster_quickstart.resources.steer_config_resource import SteerConfigResource

__all__ = [
    "DuckDBConnectionFactory",
    "HawkResource",
    "OutlookEmailResource",
    "DuckLakeCatalogBackend",
    "PostgresDuckLakeCatalogBackend",
    "RewriteDataAPIResource",
    "SteerCatalogResource",
    "SteerConfigResource",
    "create_duckdb_connection",
]
