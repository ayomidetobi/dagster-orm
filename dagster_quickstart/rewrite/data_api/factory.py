"""Factory helpers for the rewrite package."""

from __future__ import annotations

from collections.abc import Mapping

from rewrite.data_api.api.data_api import DataAPI
from rewrite.data_api.container import RewriteContainer, build_rewrite_container
from rewrite.data_api.services.vendor_service import VendorClient


def create_data_api(
    *,
    duckdb_connection: object,
    metadata_repository: object,
    value_repository: object,
    metadata_derived_repository: object | None = None,
    vendor_clients: Mapping[str, VendorClient] | None = None,
) -> DataAPI:
    """Create a DataAPI using dependency injection."""
    container = build_rewrite_container(
        duckdb_connection=duckdb_connection,
        metadata_repository=metadata_repository,
        value_repository=value_repository,
        metadata_derived_repository=metadata_derived_repository,
        vendor_clients=vendor_clients,
    )
    return container.data_api()


def create_container(
    *,
    duckdb_connection: object,
    metadata_repository: object,
    value_repository: object,
    metadata_derived_repository: object | None = None,
    vendor_clients: Mapping[str, VendorClient] | None = None,
) -> RewriteContainer:
    """Create a fully configured rewrite DI container."""
    return build_rewrite_container(
        duckdb_connection=duckdb_connection,
        metadata_repository=metadata_repository,
        value_repository=value_repository,
        metadata_derived_repository=metadata_derived_repository,
        vendor_clients=vendor_clients,
    )
