"""Direct (out-of-cache) vendor value fetch: bypasses DuckLake, hits the vendor live."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import MetadataColumns, TickerSource, ValueColumns
from dagster_quickstart.rewrite.data_api.errors import (
    InvalidOrderByError,
    MissingMetadataColumnError,
    ValueServiceRequiredError,
)
from dagster_quickstart.rewrite.data_api.services.metadata_service import MetadataService
from dagster_quickstart.rewrite.data_api.services.value_service import ValueService
from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorService
from dagster_quickstart.rewrite.data_api.shaping import melt_values
from dagster_quickstart.rewrite.data_api.vendors.derived_calc import compute_derived_series, parse_parent_series_codes
from dagster_quickstart.rewrite.data_api.vendors.ticker_columns import (
    build_series_to_ticker_map,
    resolve_ticker_field_columns,
)

logger = structlog.get_logger(__name__)

VALUE_COLUMNS = [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]


def _empty_value_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=VALUE_COLUMNS)


def sort_and_limit(
    df: pd.DataFrame,
    *,
    order_by: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Sort a long-form value frame by timestamp (then optionally order_by) and cap rows."""

    if df.empty:
        return df

    out = df.sort_values(ValueColumns.TIMESTAMP)

    if order_by:
        if order_by not in out.columns:
            logger.warning("invalid_order_by", order_by=order_by, columns=list(out.columns))
            raise InvalidOrderByError(
                f"Invalid order_by column {order_by!r}. Expected one of: {list(out.columns)}"
            )
        out = out.sort_values(order_by)

    if limit is not None:
        out = out.head(int(limit))

    return out


def get_direct_values(
    metadata_service: MetadataService,
    vendor_service: VendorService,
    series_codes: Sequence[str],
    ticker_source: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    order_by: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Fetch value rows for primary (non-derived) series directly from the vendor."""

    if not series_codes:
        return _empty_value_frame()

    ticker_col, field_col = resolve_ticker_field_columns(ticker_source)
    metadata_df = metadata_service.list_metadata({MetadataColumns.SERIES_CODE: list(series_codes)})
    if metadata_df.empty:
        return _empty_value_frame()

    required_cols = [MetadataColumns.SERIES_CODE, ticker_col, field_col]
    missing = [column for column in required_cols if column not in metadata_df.columns]
    if missing:
        logger.warning("missing_metadata_columns", ticker_source=ticker_source, missing=missing)
        raise MissingMetadataColumnError(
            f"Metadata missing required column(s) {missing} for source {ticker_source!r}"
        )

    mapping_df = metadata_df[required_cols].dropna(subset=[ticker_col]).copy()
    if mapping_df.empty:
        return _empty_value_frame()

    raw_frames: list[pd.DataFrame] = []

    if ticker_source == TickerSource.BLOOMBERG:
        # Bloomberg's TSS API fetches one field at a time across many tickers.
        for field_value, group in mapping_df.groupby(field_col):
            tickers = build_series_to_ticker_map(group, ticker_col)
            if not tickers:
                continue
            raw_frames.append(
                vendor_service.fetch(
                    ticker_source,
                    tickers=tickers,
                    field=str(field_value),
                    start=start,
                    end=end,
                )
            )
    else:
        tickers = build_series_to_ticker_map(mapping_df, ticker_col)
        if tickers:
            raw_frames.append(
                vendor_service.fetch(ticker_source, tickers=tickers, start=start, end=end)
            )

    long_frames = [
        melt_values(raw) for raw in raw_frames if raw is not None and not raw.empty
    ]
    if not long_frames:
        logger.info(
            "direct_fetch_empty", ticker_source=ticker_source, series_count=len(series_codes)
        )
        return _empty_value_frame()

    combined = pd.concat(long_frames, ignore_index=True)
    logger.info(
        "direct_fetch_completed",
        ticker_source=ticker_source,
        series_count=len(series_codes),
        row_count=len(combined),
    )
    return sort_and_limit(combined, order_by=order_by, limit=limit)


def get_derived_direct_values(
    metadata_service: MetadataService,
    derived_metadata_service: MetadataService,
    vendor_service: VendorService,
    series_codes: Sequence[str],
    ticker_source: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    value_service: ValueService | None = None,
    parents_out_of_cache: bool = False,
) -> pd.DataFrame:
    """Compute derived series from their parent series' values.

    Parent values are read from the datalake (DuckLake) by default --
    parents_out_of_cache=False -- since a derived value is usually computed
    on demand while its inputs are already cached from an earlier
    fetch-and-save. Pass parents_out_of_cache=True to instead fetch the
    parents live from the vendor too. Reading from the datalake (the
    default) requires value_service.
    """

    if not series_codes:
        return _empty_value_frame()

    requested = set(series_codes)

    dependencies_df = derived_metadata_service.list_metadata(
        {MetadataColumns.SERIES_CODE: list(series_codes)}
    )
    if dependencies_df.empty:
        return _empty_value_frame()

    parent_codes: list[str] = []
    for _, row in dependencies_df.iterrows():
        parent_codes.extend(parse_parent_series_codes(row.get(MetadataColumns.PARENT_SERIES_CODE)))
    parent_codes = list(dict.fromkeys(parent_codes))
    if not parent_codes:
        return _empty_value_frame()

    logger.info(
        "derived_direct_fetch_started",
        ticker_source=ticker_source,
        derived_count=len(series_codes),
        parent_count=len(parent_codes),
        parents_out_of_cache=parents_out_of_cache,
    )

    if parents_out_of_cache:
        parent_values = get_direct_values(
            metadata_service, vendor_service, parent_codes, ticker_source, start=start, end=end
        )
    else:
        if value_service is None:
            raise ValueServiceRequiredError(
                "Computing a derived series' parents from the datalake "
                "(parents_out_of_cache=False) requires a ValueService; pass "
                "parents_out_of_cache=True to fetch them live from the "
                "vendor instead."
            )
        parent_values = value_service.read_values(
            parent_codes, ticker_source=ticker_source, start=start, end=end
        )

    if parent_values.empty:
        return _empty_value_frame()

    parent_pivot = parent_values.pivot(
        index=ValueColumns.TIMESTAMP,
        columns=ValueColumns.SERIES_CODE,
        values=ValueColumns.VALUE,
    ).sort_index()

    result_frames: list[pd.DataFrame] = []

    for _, row in dependencies_df.iterrows():
        series_code = row.get(MetadataColumns.SERIES_CODE)
        if series_code is None or pd.isna(series_code):
            continue
        series_code = str(series_code).strip()
        if series_code not in requested:
            continue

        calc_type = row.get(MetadataColumns.CALC_TYPE, "")
        parent_series_codes = parse_parent_series_codes(row.get(MetadataColumns.PARENT_SERIES_CODE))
        if not calc_type or not parent_series_codes:
            continue

        derived_series = compute_derived_series(calc_type, parent_pivot, parent_series_codes)
        if derived_series.empty:
            continue

        derived_series.name = series_code
        long = derived_series.reset_index()
        long.columns = [ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        long[ValueColumns.SERIES_CODE] = series_code
        result_frames.append(long[VALUE_COLUMNS])

    if not result_frames:
        return _empty_value_frame()

    return pd.concat(result_frames, ignore_index=True)
