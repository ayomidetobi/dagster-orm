"""Dagster resource exposing the semantic ORM :class:`~dagster_quickstart.orm.data_api.DataAPI`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from dagster import (
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
    get_dagster_logger,
)

from dagster_quickstart.resources.duckdb_resource import DuckDBResource

if TYPE_CHECKING:
    from dagster_quickstart.orm.data_api import DataAPI

logger = get_dagster_logger()


class DataAPIResource(ConfigurableResource):
    """Dagster resource that provides a configured :class:`DataAPI` for assets and ops.

    Depends on :class:`DuckDBResource` for DuckDB + S3 Parquet access. Use
    ``context.resources.data_api.api`` (or :meth:`get_api`) inside runs.

    Example::

        @asset(required_resource_keys={"data_api"})
        def my_asset(context):
            data_api = context.resources.data_api.get_api()
            qs = data_api.get(asset_class=["Equity"])
            return qs.value()
    """

    duckdb: ResourceDependency[DuckDBResource]
    out_of_cache: bool = False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from dagster_quickstart.orm.data_api import DataAPI

        self.duckdb.setup_for_execution(context)
        self._api = DataAPI(
            duckdb_resource=self.duckdb,
            out_of_cache=self.out_of_cache,
        )
        logger.info(
            "DataAPIResource ready (out_of_cache=%s)",
            self.out_of_cache,
        )

    def get_api(self) -> DataAPI:
        """Return the :class:`DataAPI` instance for the current run."""
        api: Optional[DataAPI] = getattr(self, "_api", None)
        if api is None:
            raise RuntimeError(
                "DataAPIResource is not initialized; use only inside a Dagster run "
                "(resource setup_for_execution must have run)."
            )
        return api

    @property
    def api(self) -> DataAPI:
        """Alias for :meth:`get_api`."""
        return self.get_api()
