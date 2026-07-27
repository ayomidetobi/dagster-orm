"""Dependency-injector container for the rewrite package."""

from __future__ import annotations

from collections.abc import Mapping

from dependency_injector import containers, providers

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI, RewriteServices
from dagster_quickstart.rewrite.data_api.ingestion.file_loader import FileIngestionService
from dagster_quickstart.rewrite.data_api.ingestion.ingestion_service import IngestionService
from dagster_quickstart.rewrite.data_api.ingestion.writer import IngestionWriter
from dagster_quickstart.rewrite.data_api.repositories.metadata_repository import MetadataRepository
from dagster_quickstart.rewrite.data_api.repositories.value_repository import ValueRepository
from dagster_quickstart.rewrite.data_api.services.direct_fetch_service import DirectFetchService
from dagster_quickstart.rewrite.data_api.services.metadata_service import MetadataService
from dagster_quickstart.rewrite.data_api.services.value_service import ValueService
from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorClient, VendorService
from dagster_quickstart.rewrite.data_api.validation import validate_derived_metadata_frame


def _build_metadata_derived_service(repository: object | None) -> MetadataService | None:
    """Derived-series support is opt-in: only build the service if a repository was given."""
    if repository is None:
        return None
    return MetadataService(
        repository=MetadataRepository(repository),
        validator=validate_derived_metadata_frame,
    )


class RewriteContainer(containers.DeclarativeContainer):
    """Container wiring the rewrite package."""

    wiring_config = containers.WiringConfiguration(
        modules=[
            "dagster_quickstart.rewrite.data_api.api.data_api",
            "dagster_quickstart.rewrite.data_api.api.queryset",
            "dagster_quickstart.rewrite.data_api.ingestion.ingestion_service",
            "dagster_quickstart.rewrite.data_api.services.metadata_service",
            "dagster_quickstart.rewrite.data_api.services.value_service",
        ]
    )

    duckdb_connection = providers.Dependency()
    metadata_repository = providers.Dependency()
    value_repository = providers.Dependency()
    metadata_derived_repository = providers.Object(None)
    vendor_clients = providers.Object({})

    metadata_business_repository = providers.Factory(
        MetadataRepository,
        storage=metadata_repository,
    )

    metadata_service = providers.Factory(
        MetadataService,
        repository=metadata_business_repository,
    )

    value_business_repository = providers.Factory(
        ValueRepository,
        storage=value_repository,
    )

    value_service = providers.Factory(
        ValueService,
        repository=value_business_repository,
    )

    vendor_service = providers.Factory(
        VendorService,
        clients=vendor_clients,
    )

    metadata_derived_service = providers.Factory(
        _build_metadata_derived_service,
        repository=metadata_derived_repository,
    )

    direct_fetch_service = providers.Factory(
        DirectFetchService,
        metadata_service=metadata_service,
        vendor_service=vendor_service,
        derived_metadata_service=metadata_derived_service,
        value_service=value_service,
    )

    file_ingestion_service = providers.Factory(
        FileIngestionService,
        metadata_service=metadata_service,
        value_service=value_service,
    )

    services = providers.Factory(
        RewriteServices,
        metadata=metadata_service,
        values=value_service,
        direct_fetch=direct_fetch_service,
        ingestion=file_ingestion_service,
    )

    data_api = providers.Factory(
        DataAPI,
        services=services,
    )

    ingestion_writer = providers.Factory(
        IngestionWriter,
        service=value_service,
    )

    ingestion_service = providers.Factory(
        IngestionService,
        vendor_service=vendor_service,
        writer=ingestion_writer,
    )


def build_rewrite_container(
    *,
    duckdb_connection: object,
    metadata_repository: object,
    value_repository: object,
    metadata_derived_repository: object | None = None,
    vendor_clients: Mapping[str, VendorClient] | None = None,
) -> RewriteContainer:
    """Create and wire a rewrite container from concrete dependencies."""
    container = RewriteContainer()
    container.duckdb_connection.override(duckdb_connection)
    container.metadata_repository.override(metadata_repository)
    container.value_repository.override(value_repository)
    if metadata_derived_repository is not None:
        container.metadata_derived_repository.override(metadata_derived_repository)
    if vendor_clients is not None:
        container.vendor_clients.override(vendor_clients)
    return container
