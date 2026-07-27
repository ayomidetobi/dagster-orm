"""Dagster resource exposing the new DuckLake-backed :class:`~rewrite.data_api.api.data_api.DataAPI`.

This supersedes :class:`~dagster_quickstart.resources.data_api_resource.DataAPIResource`
(which wraps the legacy ``orm`` DataAPI) for assets migrated onto the ``rewrite``
package. Use ``context.resources.rewrite_data_api.api`` (or :meth:`get_api`) inside
runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger

if TYPE_CHECKING:
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

logger = get_dagster_logger()


class RewriteDataAPIResource(ConfigurableResource):
    """Dagster resource wrapping the rewrite DataAPI.

    Zero-config by default: ``DataAPI`` reads ``DATABASE_URL``/``S3_*`` straight
    from the environment (see ``rewrite.data_api.bootstrap``), so this resource
    needs no ``ResourceDependency`` on the legacy ``DuckDBResource``.

    ``live`` sets the default for ``out_of_cache`` on ``get_values()`` (bypass
    DuckLake and fetch straight from the vendor) -- still overridable per call.
    """

    live: bool = False

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

        self._api = DataAPI(live=self.live)
        logger.info("RewriteDataAPIResource ready (live=%s)", self.live)

    def get_api(self) -> DataAPI:
        api: Optional[DataAPI] = getattr(self, "_api", None)
        if api is None:
            raise RuntimeError(
                "RewriteDataAPIResource is not initialized; use only inside a "
                "Dagster run (context.resources.rewrite_data_api)."
            )
        return api

    @property
    def api(self) -> DataAPI:
        return self.get_api()
