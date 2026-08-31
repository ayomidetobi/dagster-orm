"""Dagster resource exposing steer.storage.SteerCatalog (silver/gold DuckLake tables)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger

if TYPE_CHECKING:
    from dagster_quickstart.steer.storage import SteerCatalog

logger = get_dagster_logger()


class SteerCatalogResource(ConfigurableResource):
    """Dagster resource wrapping SteerCatalog. Use context.resources.steer_catalog.catalog inside runs."""

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from dagster_quickstart.steer.storage import SteerCatalog

        self._catalog = SteerCatalog.build()
        self._catalog.ensure_schemas()
        logger.info("SteerCatalogResource ready (silver/gold schemas ensured)")

    @property
    def catalog(self) -> "SteerCatalog":
        instance: Optional["SteerCatalog"] = getattr(self, "_catalog", None)
        if instance is None:
            raise RuntimeError(
                "SteerCatalogResource is not initialized; use only inside a "
                "Dagster run (context.resources.steer_catalog)."
            )
        return instance
