"""Fluent, immutable query builder over metadata and value services."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime

import structlog

from dagster_quickstart.rewrite.data_api.api.requests import validate_value_query
from dagster_quickstart.rewrite.data_api.shaping import pivot_values
from dagster_quickstart.rewrite.data_api.columns import MetadataColumns, normalize_ticker_source
from dagster_quickstart.rewrite.data_api.errors import DirectFetchUnavailableError, InvalidFilterFieldError
from dagster_quickstart.rewrite.data_api.services.direct_fetch_service import DirectFetchService
from dagster_quickstart.rewrite.data_api.services.metadata_service import MetadataService
from dagster_quickstart.rewrite.data_api.services.value_service import ValueService

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class QueryState:
    """Immutable state accumulated by a QuerySet as it's built up."""

    filters: Mapping[str, Sequence[str]] = field(default_factory=dict)
    exclude: bool = False
    ticker_source: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    order_by: str | None = None
    ascending: bool = True
    limit: int | None = None
    version: int | None = None
    as_of: datetime | None = None
    out_of_cache: bool = False
    strict: bool = False
    parents_out_of_cache: bool = False


class QuerySet:
    """Fluent, immutable query builder over metadata and value services.

    Each builder method (filter/exclude/between/order_by/limit/at/live) returns
    a new QuerySet; terminal methods (metadata/value/last_value/exists) execute
    the accumulated query.
    """

    def __init__(
        self,
        metadata_service: MetadataService,
        value_service: ValueService,
        *,
        direct_fetch_service: DirectFetchService | None = None,
        state: QueryState | None = None,
    ) -> None:
        self._metadata = metadata_service
        self._values = value_service
        self._direct_fetch = direct_fetch_service
        self._state = state or QueryState()

    def filter(self, **filters: Sequence[str] | str) -> "QuerySet":
        """Return a new QuerySet narrowed by the given metadata filters."""

        return self._with_state(filters={**self._state.filters, **self._normalize(filters)})

    def exclude(self, **filters: Sequence[str] | str) -> "QuerySet":
        """Return a new QuerySet that excludes rows matching the given filters."""

        return self._with_state(
            filters={**self._state.filters, **self._normalize(filters)},
            exclude=True,
        )

    def between(self, start: datetime | None, end: datetime | None) -> "QuerySet":
        """Return a new QuerySet restricted to the given time range."""

        return self._with_state(start=start, end=end)

    def order_by(self, column: str, *, ascending: bool = True) -> "QuerySet":
        """Return a new QuerySet sorted by the given column."""

        return self._with_state(order_by=column, ascending=ascending)

    def limit(self, value: int) -> "QuerySet":
        """Return a new QuerySet capped at the given row count."""

        return self._with_state(limit=value)

    def at(self, *, version: int | None = None, as_of: datetime | None = None) -> "QuerySet":
        """Return a new QuerySet pinned to a DuckLake snapshot version or timestamp."""

        return self._with_state(version=version, as_of=as_of)

    def live(
        self,
        ticker_source: str | None = None,
        *,
        parents_out_of_cache: bool | None = None,
    ) -> "QuerySet":
        """Return a new QuerySet that bypasses DuckLake and fetches live from the vendor.

        parents_out_of_cache only matters for a derived series: it controls
        where its PARENT series' values come from -- False (default) reads
        them from the datalake, True fetches them live from the vendor too.
        """

        changes: dict[str, object] = {"out_of_cache": True}
        if ticker_source is not None:
            changes["ticker_source"] = ticker_source
        if parents_out_of_cache is not None:
            changes["parents_out_of_cache"] = parents_out_of_cache
        return self._with_state(**changes)

    def cached(self) -> "QuerySet":
        """Return a new QuerySet that reads from DuckLake instead of the vendor."""

        return self._with_state(out_of_cache=False)

    def strict(self, value: bool = True) -> "QuerySet":
        """Return a new QuerySet that raises on unrecognized filter values.

        Default is lenient (strict=False): an unrecognized filter value
        (e.g. a typo'd asset_class) is dropped with a logged warning and the
        query proceeds with the valid subset. strict=True raises
        InvalidFilterValueError instead.
        """

        return self._with_state(strict=value)

    def metadata(self):
        """Return metadata rows matching the accumulated filters."""

        return self._metadata.list_metadata(
            self._state.filters,
            exclude=self._state.exclude,
            version=self._state.version,
            as_of=self._state.as_of,
            strict=self._state.strict,
        )

    def value(self):
        """Return value rows for the series matching the accumulated filters.

        When built with .live(ticker_source), bypasses DuckLake and fetches
        live from that vendor instead of the cached values.
        """

        request = validate_value_query(
            ticker_source=self._state.ticker_source,
            start=self._state.start,
            end=self._state.end,
            order_by=self._state.order_by,
            ascending=self._state.ascending,
            limit=self._state.limit,
            version=self._state.version,
            as_of=self._state.as_of,
            out_of_cache=self._state.out_of_cache,
        )

        logger.info(
            "queryset_value",
            out_of_cache=request.out_of_cache,
            ticker_source=request.ticker_source,
        )

        if request.out_of_cache:
            if self._direct_fetch is None:
                logger.warning("direct_fetch_service_unavailable")
                raise DirectFetchUnavailableError("live() queries require a direct_fetch_service")
            frame = self._direct_fetch.get_values(
                self._series_codes(),
                request.ticker_source,
                start=request.start,
                end=request.end,
                order_by=request.order_by,
                limit=request.limit,
                parents_out_of_cache=self._state.parents_out_of_cache,
            )
        else:
            frame = self._values.read_values(
                self._series_codes(),
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

    def last_value(self):
        """Return the latest value row for each matching series."""

        ticker_source = self._state.ticker_source
        if ticker_source:
            ticker_source = normalize_ticker_source(ticker_source)

        frame = self._values.read_last_values(
            self._series_codes(),
            ticker_source=ticker_source,
        )
        return pivot_values(frame)

    def exists(self):
        """Check whether values exist for each matching series."""

        return self._values.value_exists(self._series_codes())

    def filter_options(
        self,
        fields: str | Sequence[str] | None = None,
        *,
        as_dataframe: bool = False,
    ) -> list[str] | dict[str, list[str]]:
        """Return filter values available within this QuerySet's accumulated filters.

        e.g. after .filter(asset_class=["Equity"]), filter_options("currency")
        returns only the currencies that appear among Equity series.
        """

        return self._metadata.filter_options(
            fields,
            filters=self._state.filters,
            exclude=self._state.exclude,
            strict=self._state.strict,
            as_dataframe=as_dataframe,
        )

    def groupby(self, by: str | Sequence[str]) -> Iterator[tuple[tuple, "QuerySet"]]:
        """Group this QuerySet's matching metadata by one or more columns.

        Yields (group_key, QuerySet) pairs -- group_key is always a tuple
        (single-element when `by` is a single column), and each yielded
        QuerySet is scoped to that group's series codes so it can be used
        like any other QuerySet:

            for (asset_class,), group in data_api.query().groupby("asset_class"):
                print(asset_class, group.value())
        """

        columns = [by] if isinstance(by, str) else list(by)
        if not columns:
            raise ValueError("groupby() requires at least one column")

        available_columns = self._metadata.list_columns()
        invalid_columns = [column for column in columns if column not in available_columns]
        if invalid_columns:
            logger.warning(
                "invalid_groupby_column",
                invalid_columns=invalid_columns,
                available_columns=sorted(available_columns),
            )
            raise InvalidFilterFieldError(
                f"Invalid groupby column(s): {invalid_columns}. "
                f"Available columns: {sorted(available_columns)}"
            )

        metadata_df = self.metadata()
        if metadata_df.empty:
            return

        for group_key, group_df in metadata_df.groupby(columns, dropna=False):
            if not isinstance(group_key, tuple):
                group_key = (group_key,)

            series_codes = (
                group_df[MetadataColumns.SERIES_CODE].dropna().astype(str).str.strip().unique().tolist()
            )
            if not series_codes:
                continue

            yield group_key, self._with_state(
                filters={MetadataColumns.SERIES_CODE: series_codes},
                exclude=False,
            )

    def _series_codes(self) -> list[str]:
        return self._metadata.resolve_series_codes(
            self._state.filters,
            exclude=self._state.exclude,
            strict=self._state.strict,
        )

    @staticmethod
    def _normalize(filters: Mapping[str, Sequence[str] | str]) -> dict[str, list[str]]:
        return {
            key: [value] if isinstance(value, str) else list(value)
            for key, value in filters.items()
        }

    def _with_state(self, **changes: object) -> "QuerySet":
        return QuerySet(
            self._metadata,
            self._values,
            direct_fetch_service=self._direct_fetch,
            state=replace(self._state, **changes),
        )
