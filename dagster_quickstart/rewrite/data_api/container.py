"""Dependency-injector container for the rewrite package."""

from __future__ import annotations

from collections.abc import Mapping

from dependency_injector import containers, providers

from rewrite.data_api.api.data_api import DataAPI, RewriteServices
from rewrite.data_api.ingestion.file_loader import FileIngestionService
from rewrite.data_api.ingestion.ingestion_service import IngestionService
from rewrite.data_api.ingestion.writer import IngestionWriter
from rewrite.data_api.repositories.metadata_repository import MetadataRepository
from rewrite.data_api.repositories.value_repository import ValueRepository
from rewrite.data_api.services.direct_fetch_service import DirectFetchService
from rewrite.data_api.services.metadata_service import MetadataService
from rewrite.data_api.services.value_service import ValueService
from rewrite.data_api.services.vendor_service import VendorClient, VendorService
from rewrite.data_api.validation import validate_derived_metadata_frame


def _build_metadata_derived_service(repository: object | None) -> MetadataService | None:
    """Derived-series support is opt-in: only build the service if a repository was given."""
    if repository is None:
        return None
    return MetadataService(
        repository=MetadataRepository(repository),
        validator=validate_derived_metadata_frame,
    )


def _build_file_ingestion_service(
    metadata_service: MetadataService,
    value_service: ValueService,
    cacher: object | None,
) -> FileIngestionService | None:
    """File ingestion is opt-in: only build the service if a cacher was given."""
    if cacher is None:
        return None
    return FileIngestionService(
        metadata_service=metadata_service,
        value_service=value_service,
        cacher=cacher,
    )


class RewriteContainer(containers.DeclarativeContainer):
    """Container wiring the rewrite package."""

    wiring_config = containers.WiringConfiguration(
        modules=[
            "rewrite.data_api.api.data_api",
            "rewrite.data_api.api.queryset",
            "rewrite.data_api.ingestion.ingestion_service",
            "rewrite.data_api.services.metadata_service",
            "rewrite.data_api.services.value_service",
        ]
    )

    duckdb_connection = providers.Dependency()
    metadata_repository = providers.Dependency()
    value_repository = providers.Dependency()
    duckdb_data_cacher = providers.Object(None)
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
    )

    file_ingestion_service = providers.Factory(
        _build_file_ingestion_service,
        metadata_service=metadata_service,
        value_service=value_service,
        cacher=duckdb_data_cacher,
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
    duckdb_data_cacher: object | None = None,
    metadata_derived_repository: object | None = None,
    vendor_clients: Mapping[str, VendorClient] | None = None,
) -> RewriteContainer:
    """Create and wire a rewrite container from concrete dependencies."""
    container = RewriteContainer()
    container.duckdb_connection.override(duckdb_connection)
    container.metadata_repository.override(metadata_repository)
    container.value_repository.override(value_repository)
    if duckdb_data_cacher is not None:
        container.duckdb_data_cacher.override(duckdb_data_cacher)
    if metadata_derived_repository is not None:
        container.metadata_derived_repository.override(metadata_derived_repository)
    if vendor_clients is not None:
        container.vendor_clients.override(vendor_clients)
    return container
