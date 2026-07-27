"""Dagster resources for the financial platform."""

# from dagster_quickstart.resources.data_api_resource import DataAPIResource
# from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import (
    DuckDBConnectionFactory,
    # DuckDBDataCacher,
    DuckLakeCatalogBackend,
    PostgresDuckLakeCatalogBackend,
    create_duckdb_connection,
)
from dagster_quickstart.resources.hawk_resources import HawkResource
from dagster_quickstart.resources.outlook_email_resource import OutlookEmailResource
from dagster_quickstart.resources.rewrite_data_api_resource import RewriteDataAPIResource

__all__ = [
    # "DataAPIResource",
    "DuckDBConnectionFactory",
    "DuckDBDataCacher",
    # "DuckDBResource",
    "HawkResource",
    "OutlookEmailResource",
    "DuckLakeCatalogBackend",
    "PostgresDuckLakeCatalogBackend",
    "RewriteDataAPIResource",
    "create_duckdb_connection",
]
