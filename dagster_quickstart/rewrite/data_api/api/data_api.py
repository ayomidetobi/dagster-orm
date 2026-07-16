"""Public DataAPI for the DuckLake rewrite package."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import structlog

from rewrite.data_api.api.metadata_result import MetadataResult
from rewrite.data_api.api.queryset import QueryState, QuerySet
from rewrite.data_api.api.requests import validate_value_query
from rewrite.data_api.api.shaping import pivot_values
from rewrite.data_api.columns import ValueColumns, normalize_ticker_source
from rewrite.data_api.services.direct_fetch_service import DirectFetchService
from rewrite.data_api.services.metadata_service import MetadataService
from rewrite.data_api.services.value_service import ValueService
from rewrite.data_api.services.vendor_service import VendorClient

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RewriteServices:
    """Bundles the services that back the public DataAPI."""

    metadata: MetadataService
    values: ValueService
    direct_fetch: DirectFetchService


class DataAPI:
    """Public facade over the rewrite metadata/value services.

    Easiest use -- reads DATABASE_URL/S3_* from the environment and wires
    everything (DuckLake connection, repositories, vendor clients) under the
    hood:

        data_api = DataAPI(live=True)
        data_api.get_values(["SX0001_PX_LAST"], ticker_source="BBG")

    `live` sets the default for out_of_cache on get_values() (bypass DuckLake
    and fetch straight from the vendor) -- still overridable per call. Pass
    `services=` explicitly (e.g. via rewrite.data_api.factory.create_data_api())
    for DI-wired construction/testing instead of the environment-based default.
    """

    def __init__(
        self,
        *,
        services: RewriteServices | None = None,
        live: bool = False,
        vendor_clients: Mapping[str, VendorClient] | None = None,
    ) -> None:
        if services is None:
            from rewrite.data_api.bootstrap import build_default_services

            services = build_default_services(vendor_clients=vendor_clients)
        self._services = services
        self._live = live

    def query(self) -> QuerySet:
        """Start a fluent query against metadata and values."""
        return QuerySet(
            self._services.metadata,
            self._services.values,
            direct_fetch_service=self._services.direct_fetch,
            state=QueryState(out_of_cache=self._live),
        )

    def get_metadata(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
        strict: bool = False,
        **field_filters: Sequence[str] | str,
    ) -> MetadataResult:
        """Return metadata rows matching the requested filters.

        Filters can be passed as a dict, as keyword arguments, or both (kwargs
        win on key collision):

            data_api.get_metadata(asset_class=["Equity", "Commodity"])

        The result can fetch values for the matched series directly, without
        re-extracting series_code yourself:

            context = data_api.get_metadata(asset_class=["Equity"])
            values = context.get_values(start=start, ticker_source="BBG")
            latest = context.get_last_values(ticker_source="BBG")

        strict controls how unrecognized filter *values* (e.g. a typo'd
        asset_class) are handled: False (default) drops them with a logged
        warning and proceeds with the valid subset; True raises
        InvalidFilterValueError naming the bad value(s) and the valid options.
        """
        merged_filters = self._merge_filters(filters, field_filters)
        frame = self._services.metadata.list_metadata(
            merged_filters,
            exclude=exclude,
            version=version,
            as_of=as_of,
            strict=strict,
        )
        return MetadataResult(
            frame,
            fetch_values=self.get_values,
            fetch_last_values=self.get_last_values,
        )

    @staticmethod
    def _merge_filters(
        filters: Mapping[str, Sequence[str]] | None,
        field_filters: Mapping[str, Sequence[str] | str],
    ) -> dict[str, list[str]]:
        merged = dict(filters) if filters else {}
        merged.update(
            {
                key: [value] if isinstance(value, str) else list(value)
                for key, value in field_filters.items()
            }
        )
        return merged

    def get_metadata_columns(self) -> list[str]:
        """Return the available metadata column names (valid filter keys for get_metadata())."""
        return self._services.metadata.list_columns()

    def filter_options(
        self,
        fields: str | Sequence[str] | None = None,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
        strict: bool = False,
        as_dataframe: bool = False,
    ) -> list[str] | dict[str, list[str]] | pd.DataFrame:
        """Return available metadata filter values.

        Use this to discover what you can filter get_metadata()/get_values()
        by. fields=None returns options for every column; pass filters to
        narrow the options to a subset (e.g. currency values within
        asset_class=Equity). Passing an unknown field raises
        InvalidFilterFieldError listing the valid columns; an unrecognized
        value in the narrowing `filters` is handled per `strict` (see
        get_metadata()).
        """
        return self._services.metadata.filter_options(
            fields,
            filters=filters,
            exclude=exclude,
            strict=strict,
            as_dataframe=as_dataframe,
        )

    def import_metadata(self, frame: pd.DataFrame) -> None:
        """Persist a normalized metadata frame."""
        self._services.metadata.import_metadata(frame)

    def refresh_metadata(self) -> None:
        """Refresh repository-backed metadata state."""
        self._services.metadata.refresh_metadata()

    def get_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        order_by: str | None = ValueColumns.TIMESTAMP,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
        out_of_cache: bool | None = None,
    ) -> pd.DataFrame:
        """Return value rows for the requested series.

        ticker_source accepts vendor abbreviations (e.g. "BBG" for Bloomberg),
        case-insensitively -- see rewrite.data_api.columns.normalize_ticker_source.

        out_of_cache defaults to the `live` flag this DataAPI was constructed
        with; pass it explicitly to override for a single call. When true,
        bypasses DuckLake entirely and fetches live from the vendor named by
        ticker_source (required in that case).
        """
        request = validate_value_query(
            ticker_source=ticker_source,
            start=start,
            end=end,
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            version=version,
            as_of=as_of,
            out_of_cache=self._live if out_of_cache is None else out_of_cache,
        )

        logger.info(
            "data_api_get_values",
            series_count=len(series_codes),
            out_of_cache=request.out_of_cache,
            ticker_source=request.ticker_source,
        )

        if request.out_of_cache:
            frame = self._services.direct_fetch.get_values(
                series_codes,
                request.ticker_source,
                start=request.start,
                end=request.end,
                order_by=request.order_by,
                limit=request.limit,
            )
        else:
            frame = self._services.values.read_values(
                series_codes,
                ticker_source=request.ticker_source,
                start=request.start,
                end=request.end,
                order_by=request.order_by,
                ascending=request.ascending,
                limit=request.limit,
                version=request.version,
                as_of=request.as_of,
            )

        return pivot_values(frame)

    def get_last_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
    ) -> pd.DataFrame:
        """Return the latest value row for each requested series.

        Always reads from DuckLake -- the `live` flag doesn't apply here,
        since "latest value, live" isn't a vendor operation.
        """
        if ticker_source:
            ticker_source = normalize_ticker_source(ticker_source)
        frame = self._services.values.read_last_values(series_codes, ticker_source=ticker_source)
        return pivot_values(frame)

    def write_values(self, frame: pd.DataFrame) -> None:
        """Persist a normalized value frame."""
        self._services.values.write_values(frame)

    def value_exists(self, series_codes: Sequence[str]) -> Mapping[str, bool]:
        """Check whether value rows exist for the requested series."""
        return self._services.values.value_exists(series_codes)
