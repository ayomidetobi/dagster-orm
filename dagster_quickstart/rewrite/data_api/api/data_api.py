"""Public DataAPI for the DuckLake rewrite package."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.api.metadata_result import MetadataResult
from dagster_quickstart.rewrite.data_api.api.queryset import QueryState, QuerySet
from dagster_quickstart.rewrite.data_api.api.requests import validate_value_query
from dagster_quickstart.rewrite.data_api.columns import ValueColumns, normalize_ticker_source
from dagster_quickstart.rewrite.data_api.shaping import melt_values, pivot_values
from dagster_quickstart.rewrite.data_api.errors import IngestionUnavailableError, InvalidImportSourceError
from dagster_quickstart.rewrite.data_api.ingestion.file_loader import FileIngestionService
from dagster_quickstart.rewrite.data_api.repositories.generic_table_repository import (
    GenericTableRepository,
)
from dagster_quickstart.rewrite.data_api.services.direct_fetch_service import DirectFetchService
from dagster_quickstart.rewrite.data_api.services.metadata_service import MetadataService
from dagster_quickstart.rewrite.data_api.services.value_service import ValueService
from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorClient

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RewriteServices:
    """Bundles the services that back the public DataAPI."""

    metadata: MetadataService
    values: ValueService
    direct_fetch: DirectFetchService
    tables: GenericTableRepository
    ingestion: FileIngestionService | None = None


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
            from dagster_quickstart.rewrite.data_api.bootstrap import build_default_services

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

    def get_metadata_exclude(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        version: int | None = None,
        as_of: datetime | None = None,
        strict: bool = False,
        **field_filters: Sequence[str] | str,
    ) -> MetadataResult:
        """Return metadata rows that do NOT match the requested filters.

        Same filter syntax as get_metadata() -- dict, kwargs, or both:

            data_api.get_metadata_exclude(asset_class=["Equity"])

        returns every series whose asset_class isn't Equity. Equivalent to
        get_metadata(..., exclude=True); see get_metadata() for `strict`.
        """
        return self.get_metadata(
            filters,
            exclude=True,
            version=version,
            as_of=as_of,
            strict=strict,
            **field_filters,
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

    def import_metadata(
        self,
        frame: pd.DataFrame | None = None,
        *,
        path: str | Path | None = None,
        sheet: str | int | None = None,
        fresh: bool = False,
    ) -> pd.DataFrame:
        """Persist metadata -- either an in-memory frame, or a CSV/Excel file.

            data_api.import_metadata(path="meta_series.csv")
            data_api.import_metadata(path="meta_series.xlsx", sheet="abc")
            data_api.import_metadata(frame=df)

        Exactly one of `frame`/`path` must be given. `sheet` selects a sheet
        by name or index for Excel files; ignored for CSV. Writes go
        straight into the DuckLake metadata table -- the result returned is
        the validated rows, so it can be queried right back with
        get_metadata().

        fresh controls what happens to existing rows for the series_codes
        in this import:

        - fresh=False (default): appends, exactly as before. Re-importing
          the same file duplicates every one of its series_codes, since
          DuckLake is append-only by design and nothing here dedupes.
        - fresh=True: deletes any existing rows for this import's
          series_codes first, then inserts -- so re-importing the same file
          replaces those rows instead of duplicating them. series_codes
          belonging to OTHER, previously-imported files are untouched, so
          importing several distinct files into one catalog still works;
          only a series_code actually present in *this* import gets
          replaced. Delete and insert happen in the same transaction.
        """
        if (frame is None) == (path is None):
            raise InvalidImportSourceError(
                "import_metadata() requires exactly one of `frame` or `path`."
            )

        if path is not None:
            if self._services.ingestion is None:
                raise IngestionUnavailableError(
                    "import_metadata(path=...) requires a DataAPI built with "
                    "file-ingestion support (e.g. DataAPI(live=True) or "
                    "DataAPI()) -- this instance has none configured."
                )
            return self._services.ingestion.ingest_metadata_file(path, sheet=sheet, fresh=fresh)

        return self._services.metadata.import_metadata(frame, fresh=fresh)

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
        parents_out_of_cache: bool = False,
    ) -> pd.DataFrame:
        """Return value rows for the requested series.

        ticker_source accepts vendor abbreviations (e.g. "BBG" for Bloomberg),
        case-insensitively -- see rewrite.data_api.columns.normalize_ticker_source.

        out_of_cache defaults to the `live` flag this DataAPI was constructed
        with; pass it explicitly to override for a single call. When true,
        bypasses DuckLake entirely and fetches live from the vendor named by
        ticker_source (required in that case).

        parents_out_of_cache only matters for a derived series requested
        with out_of_cache=True: it controls where its PARENT series' values
        come from -- False (default) reads them from the datalake
        (DuckLake), True fetches them live from the vendor too. Has no
        effect on non-derived series or on out_of_cache=False calls.

        The returned frame remembers its ticker_source (in `.attrs`), so
        write_values(get_values(...)) tags rows with the right vendor
        automatically instead of writing them unattributed.
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
                parents_out_of_cache=parents_out_of_cache,
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

        result = pivot_values(frame)
        if request.ticker_source:
            result.attrs["ticker_source"] = request.ticker_source
        return result

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
        result = pivot_values(frame)
        if ticker_source:
            result.attrs["ticker_source"] = ticker_source
        return result

    def write_values(self, frame: pd.DataFrame) -> None:
        """Persist a value frame -- long form or wide form, detected automatically.

        Long form (series_code, timestamp, value columns) is written as-is.
        Wide form (a timestamp DatetimeIndex, one column per series_code --
        exactly what get_values()/get_last_values() return) is melted back
        to long form first, so a round trip like

            values = data_api.get_values(series_codes, ticker_source="BBG")
            data_api.write_values(values)

        works with no manual reshaping -- and tags the melted rows with
        "bloomberg" automatically, via the ticker_source get_values() left
        in `values.attrs`, rather than writing them unattributed. Detected
        by the presence of a series_code column: long-form frames always
        have one, wide-form frames never do. A frame with neither a
        series_code column nor a DatetimeIndex is passed through unchanged
        and left to fail validation with a clear error naming what's
        missing.
        """
        if ValueColumns.SERIES_CODE not in frame.columns and isinstance(
            frame.index, pd.DatetimeIndex
        ):
            frame = melt_values(frame, ticker_source=frame.attrs.get("ticker_source"))
        self._services.values.write_values(frame)

    def value_exists(self, series_codes: Sequence[str]) -> Mapping[str, bool]:
        """Check whether value rows exist for the requested series."""
        return self._services.values.value_exists(series_codes)

    def get_values_storage_path(self) -> str | None:
        """Return the common S3/local path DuckLake is currently using for the values table.

        Queried live via ducklake_list_files() -- always reflects reality
        (partitioning, bucket, prefix) rather than an assumed convention.
        None if nothing has been written yet.
        """
        return self._services.values.get_storage_path()

    def read_table(
        self,
        schema: str,
        table: str,
        *,
        strict: bool = False,
        **filters: Sequence[str] | str,
    ) -> MetadataResult:
        """Read an arbitrary DuckLake table, optionally filtered by column values.

        For tables outside the metadata/values schema -- STEER's silver/gold tables, for
        instance (see steer/results.py, steer/model.py). Returns a MetadataResult, so results
        chain, narrow further in-memory, and expose filter_options()/union() the same way
        get_metadata() does:

            results = data_api.read_table("gold", "steer_result_summary", universe="G10")
            results.get_metadata(series_code="EURNOK_PX_LAST").filter_options("as_of")

        strict controls how an unrecognized filter value is handled, same as get_metadata()
        (False drops it with a logged warning, True raises InvalidFilterValueError) --
        "valid options" here means the values actually present in the table.

        Returns an empty MetadataResult if the table doesn't exist yet, rather than raising.
        get_values()/get_last_values() aren't meaningful on this result -- there's no
        series_code/value-column convention for an arbitrary table -- so calling either raises
        a clear NotImplementedError naming this table, instead of failing deeper and less
        clearly inside MetadataResult.
        """
        frame = self._services.tables.read_all(schema, table)
        result = MetadataResult(
            frame,
            fetch_values=_unsupported_on_table(schema, table, "get_values"),
            fetch_last_values=_unsupported_on_table(schema, table, "get_last_values"),
        )
        if filters and not frame.empty:
            result = result.get_metadata(strict=strict, **filters)
        return result

    def write_table(self, schema: str, table: str, frame: pd.DataFrame) -> None:
        """Append `frame` to an arbitrary DuckLake table, creating it on first write.

        Append-only, like the rest of this catalog. If `frame` has columns the existing table
        lacks, the table is widened with ALTER TABLE ADD COLUMN first, then the rows are
        appended -- so a wider driver set (e.g. CHN's 7 coefficient columns vs G10's 5) doesn't
        misalign, and no existing row is ever rewritten (see
        rewrite.data_api.repositories.generic_table_repository.GenericTableRepository.write()).
        """
        self._services.tables.write(schema, table, frame)


def _unsupported_on_table(schema: str, table: str, method: str):
    """A fetch_values/fetch_last_values stand-in for read_table()'s MetadataResult.

    Raises a clear error naming the method and table if actually called, rather than leaving
    fetch_values=None (which would fail later with an unhelpful TypeError deep inside
    MetadataResult.get_values()/get_last_values()).
    """

    def _raise(*_args: object, **_kwargs: object) -> pd.DataFrame:
        raise NotImplementedError(
            f"{method}() is not supported on the result of read_table({schema!r}, {table!r}) -- "
            "there is no series_code/value-column convention for an arbitrary table. Read "
            "whatever you need directly from the result's .frame instead."
        )

    return _raise
