"""Wrapper around a metadata query result that can fetch its own values.

Lets a caller go straight from a metadata filter to values for the matched
series, without manually pulling series_code back out and re-passing it:

    context = data_api.get_metadata(asset_class=["Equity", "Commodity"])
    values = context.get_values(start=start, ticker_source="BBG")
    latest = context.get_last_values(ticker_source="BBG")

It's also chainable: get_metadata()/get_metadata_exclude() called on a
MetadataResult narrow that result further (in-memory, no new database
query), so filters can be composed step by step:

    data_api.get_metadata(sub_asset_class="stocks") \\
        .get_metadata(region="north america") \\
        .get_metadata_exclude(currency="USD")

Otherwise behaves like the underlying DataFrame (indexing, iteration, len,
repr, attribute access) via delegation, so existing pd.DataFrame-shaped
callers of get_metadata() keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd
import structlog

from rewrite.data_api.columns import MetadataColumns
from rewrite.data_api.errors import InvalidFilterFieldError, InvalidFilterValueError

logger = structlog.get_logger(__name__)


class MetadataResult:
    """A metadata DataFrame that can fetch values for its own series codes."""

    def __init__(
        self,
        frame: pd.DataFrame,
        *,
        fetch_values: Callable[..., pd.DataFrame],
        fetch_last_values: Callable[..., pd.DataFrame],
    ) -> None:
        self._frame = frame
        self._fetch_values = fetch_values
        self._fetch_last_values = fetch_last_values

    @property
    def frame(self) -> pd.DataFrame:
        """The underlying metadata DataFrame."""
        return self._frame

    @property
    def info(self) -> pd.DataFrame:
        """The underlying metadata DataFrame (alias for `.frame`).

        Defined as a real property so it returns the metadata rows rather
        than falling through __getattr__ to pandas' DataFrame.info(), which
        prints a dtype/memory summary instead of the data.
        """
        return self._frame

    @property
    def series_codes(self) -> list[str]:
        """The distinct, non-empty series codes present in this result."""
        if self._frame.empty or MetadataColumns.SERIES_CODE not in self._frame.columns:
            return []
        codes = self._frame[MetadataColumns.SERIES_CODE].dropna().astype(str).str.strip()
        return [code for code in dict.fromkeys(codes.tolist()) if code]

    def get_metadata(
        self,
        filters: Mapping[str, Sequence[str] | str] | None = None,
        *,
        strict: bool = False,
        **field_filters: Sequence[str] | str,
    ) -> "MetadataResult":
        """Narrow this result further by additional filters.

        Filters are applied against the rows already in this result (no new
        database query), so calls chain naturally:

            data_api.get_metadata(sub_asset_class="stocks") \\
                .get_metadata(region="north america") \\
                .get_metadata_exclude(currency="USD")

        strict controls how an unrecognized filter value is handled, same
        as DataAPI.get_metadata() -- except "valid options" here means the
        values actually present in this (already narrowed) result.
        """
        return self._narrow(filters, field_filters, exclude=False, strict=strict)

    def get_metadata_exclude(
        self,
        filters: Mapping[str, Sequence[str] | str] | None = None,
        *,
        strict: bool = False,
        **field_filters: Sequence[str] | str,
    ) -> "MetadataResult":
        """Narrow this result by excluding rows matching additional filters.

        Equivalent to get_metadata(..., exclude=True) at this point in a
        chain; see get_metadata() for chaining/strict semantics.
        """
        return self._narrow(filters, field_filters, exclude=True, strict=strict)

    def _narrow(
        self,
        filters: Mapping[str, Sequence[str] | str] | None,
        field_filters: Mapping[str, Sequence[str] | str],
        *,
        exclude: bool,
        strict: bool,
    ) -> "MetadataResult":
        merged = dict(filters) if filters else {}
        merged.update(
            {
                key: [value] if isinstance(value, str) else list(value)
                for key, value in field_filters.items()
            }
        )

        frame = self._frame
        for field, values in merged.items():
            frame = self._apply_field_filter(frame, field, values, exclude=exclude, strict=strict)

        return MetadataResult(
            frame.reset_index(drop=True),
            fetch_values=self._fetch_values,
            fetch_last_values=self._fetch_last_values,
        )

    @staticmethod
    def _apply_field_filter(
        frame: pd.DataFrame,
        field: str,
        values: list[str],
        *,
        exclude: bool,
        strict: bool,
    ) -> pd.DataFrame:
        if field not in frame.columns:
            available_columns = sorted(frame.columns)
            raise InvalidFilterFieldError(
                f"Invalid field(s): [{field!r}]. Available columns: {available_columns}"
            )

        valid_values = frame[field].dropna().unique().tolist()
        invalid_values = [value for value in values if value not in valid_values]

        if invalid_values:
            if strict:
                logger.warning(
                    "invalid_filter_value",
                    field=field,
                    invalid_values=invalid_values,
                    available_options=sorted(valid_values),
                )
                raise InvalidFilterValueError(
                    f"Invalid value(s) for field {field!r}: {invalid_values}. "
                    f"Valid options: {sorted(valid_values)}"
                )

            logger.warning(
                "invalid_filter_value_dropped",
                field=field,
                invalid_values=invalid_values,
                available_options=sorted(valid_values),
            )
            values = [value for value in values if value in valid_values]

        mask = frame[field].isin(values)
        return frame[~mask if exclude else mask]

    def filter_options(
        self,
        fields: str | Sequence[str] | None = None,
        *,
        as_dataframe: bool = False,
    ) -> list[str] | dict[str, list[str]] | pd.DataFrame:
        """Return the distinct values available for column(s) in this result.

        Chains off get_metadata()/get_metadata_exclude() the same way they
        chain off each other -- computed from this result's already-fetched
        rows (no new database query), so options reflect however this result
        has already been narrowed:

            data_api.get_metadata(asset_class=["Equity"]) \\
                .filter_options("currency")

        returns only the currencies that actually appear among Equity
        series. fields=None returns options for every column. Passing an
        unknown field raises InvalidFilterFieldError listing the valid
        columns.
        """
        available_columns = list(self._frame.columns)

        if fields is None:
            requested_fields = available_columns
        else:
            requested_fields = [fields] if isinstance(fields, str) else list(fields)

        if not requested_fields:
            raise ValueError("filter_options() requires at least one field")

        invalid_fields = [field for field in requested_fields if field not in available_columns]
        if invalid_fields:
            raise InvalidFilterFieldError(
                f"Invalid field(s): {invalid_fields}. Available columns: {sorted(available_columns)}"
            )

        options_by_field = {
            field: sorted(
                value
                for value in self._frame[field].dropna().astype(str).str.strip().unique().tolist()
                if value
            )
            for field in requested_fields
        }

        if fields is not None and len(requested_fields) == 1:
            options = options_by_field[requested_fields[0]]
            if not as_dataframe:
                return options
            options_by_field = {requested_fields[0]: options}

        if not as_dataframe:
            return options_by_field

        rows = [
            {"field": field, "value": value}
            for field, values in options_by_field.items()
            for value in values
        ]
        return pd.DataFrame(rows, columns=["field", "value"])

    def union(self, *others: "MetadataResult | pd.DataFrame") -> "MetadataResult":
        """Combine this result with one or more other metadata results.

        Rows are combined and deduplicated by series_code (first occurrence
        wins), so results don't need to share filters or even come from the
        same query:

            equity = data_api.get_metadata(asset_class=["Equity"])
            commodity = data_api.get_metadata(asset_class=["Commodity"])
            combined = equity.union(commodity)

        Accepts any number of MetadataResults, or plain DataFrames, in one
        call: equity.union(commodity, fixed_income). Called with nothing,
        returns this result unchanged.
        """
        if not others:
            return self

        frames = [self._frame] + [
            other.frame if isinstance(other, MetadataResult) else other for other in others
        ]
        combined = pd.concat(frames, ignore_index=True)

        if MetadataColumns.SERIES_CODE in combined.columns:
            combined = combined.drop_duplicates(subset=[MetadataColumns.SERIES_CODE], keep="first")

        return MetadataResult(
            combined.reset_index(drop=True),
            fetch_values=self._fetch_values,
            fetch_last_values=self._fetch_last_values,
        )

    def get_values(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch values for this result's series codes.

        Keyword arguments are forwarded to DataAPI.get_values() (e.g.
        ticker_source, start, end, order_by, limit, out_of_cache).
        """
        return self._fetch_values(self.series_codes, **kwargs)

    def get_last_values(self, *, ticker_source: str | None = None) -> pd.DataFrame:
        """Fetch the latest value row for each of this result's series."""
        return self._fetch_last_values(self.series_codes, ticker_source=ticker_source)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._frame, name)

    def __getitem__(self, key: Any) -> Any:
        return self._frame[key]

    def __len__(self) -> int:
        return len(self._frame)

    def __iter__(self):
        return iter(self._frame)

    def __repr__(self) -> str:
        return repr(self._frame)
