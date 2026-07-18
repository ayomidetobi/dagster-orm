"""Vendor ticker/field column resolution for metadata-driven direct fetches."""

from __future__ import annotations

import pandas as pd
import structlog

from rewrite.data_api.columns import MetadataColumns, TickerSource
from rewrite.data_api.errors import UnsupportedTickerSourceError

logger = structlog.get_logger(__name__)

VENDOR_TICKER_COLUMNS: dict[str, tuple[str, str]] = {
    TickerSource.BLOOMBERG: ("bbg_ticker", "bbg_field"),
    TickerSource.HAWK: ("hawk_ticker", "hawk_field"),
    TickerSource.MDS: ("mds_ticker", "mds_field"),
}


def resolve_ticker_field_columns(ticker_source: str) -> tuple[str, str]:
    """Return the (ticker_column, field_column) pair for a vendor's metadata."""

    try:
        return VENDOR_TICKER_COLUMNS[ticker_source]
    except KeyError:
        logger.warning("unsupported_ticker_source", ticker_source=ticker_source)
        raise UnsupportedTickerSourceError(
            f"Direct fetch not supported for ticker source {ticker_source!r}. "
            f"Expected one of: {sorted(VENDOR_TICKER_COLUMNS)}"
        ) from None


def build_series_to_ticker_map(
    metadata_df: pd.DataFrame,
    ticker_column: str,
) -> dict[str, str]:
    """Build a series_code -> vendor ticker map from a metadata DataFrame."""

    if metadata_df.empty or ticker_column not in metadata_df.columns:
        return {}

    mapping: dict[str, str] = {}

    for _, row in metadata_df.iterrows():
        series_code = row.get(MetadataColumns.SERIES_CODE)
        ticker = row.get(ticker_column)

        if series_code is None or pd.isna(series_code):
            continue

        if ticker is None or pd.isna(ticker) or not str(ticker).strip():
            continue

        mapping[str(series_code).strip()] = str(ticker).strip()

    return mapping
