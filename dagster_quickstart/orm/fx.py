"""FX-scoped semantic ORM API."""

from typing import Any, Dict, Optional, Unpack

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.schema import FilterParams, MetadataColumns
from dagster_quickstart.resources.duckdb_resource import DuckDBResource


class FX(DataAPI):
    """Asset-specific DataAPI scoped to ``asset_class="FX"``.

    This class inherits all ``DataAPI`` behavior, including repository wiring,
    lookup helpers, and DataAPI-level ``out_of_cache`` defaults.

    Examples:
        fx = FX(out_of_cache=True)
        usa_fx = fx.get(country="USA")
        values = usa_fx.value()

        # Equivalent to:
        # DataAPI(out_of_cache=True).get(asset_class="FX", country="USA").value()
    """

    _ASSET_CLASS = "FX"

    def __init__(
        self,
        duckdb_resource: Optional[DuckDBResource] = None,
        out_of_cache: bool = False,
    ):
        """Initialize an FX-scoped DataAPI.

        Args:
            duckdb_resource: DuckDBResource instance with connection and S3 access configured.
            out_of_cache: Default ``out_of_cache`` behavior for QuerySets created from
                this FX instance.
        """
        super().__init__(duckdb_resource=duckdb_resource, out_of_cache=out_of_cache)

    @classmethod
    def _validate_asset_class_override(cls, filters: Dict[str, Any]) -> None:
        """Ensure callers do not override the fixed asset class scope."""
        if "asset_class" in filters:
            raise ValueError("`asset_class` is predefined by FX and cannot be overridden.")

    def get(self, **filters: Unpack[FilterParams]) -> QuerySet:
        """Create an FX-scoped QuerySet.

        Args:
            **filters: Metadata filters to apply within the FX asset class universe.

        Returns:
            QuerySet restricted to ``asset_class="FX"`` plus any additional filters.

        Raises:
            ValueError: If ``asset_class`` is passed explicitly.
        """
        filters = dict(filters)
        self._validate_asset_class_override(filters)
        filters[MetadataColumns.ASSET_CLASS] = self._ASSET_CLASS
        return super().get(**filters)

    def get_excluding(self, **filters: Unpack[FilterParams]) -> QuerySet:
        """Create an FX-scoped QuerySet with exclusions applied inside the FX universe.

        Args:
            **filters: Metadata filters to exclude from the FX-scoped base queryset.

        Returns:
            QuerySet representing FX rows excluding the provided filters.

        Raises:
            ValueError: If ``asset_class`` is passed explicitly.
        """
        filters = dict(filters)
        self._validate_asset_class_override(filters)
        control_table = filters.pop("control_table", None)
        return self.get(control_table=control_table).filter_exclude(**filters)
