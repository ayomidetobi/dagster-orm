"""Shared helpers for mapping series codes and vendor tickers."""

from typing import Dict

import pandas as pd  # type: ignore[import-untyped]

from dagster_quickstart.orm.schema import (
    MetadataColumns,
    TickerSource,
    get_vendor_ticker_column,
)


def build_series_to_ticker_map(
    metadata_df: pd.DataFrame,
    ticker_source: TickerSource,
) -> Dict[str, str]:
    """Build ``series_code -> ticker`` map for a given vendor source."""
    ticker_column = get_vendor_ticker_column(ticker_source)
    if ticker_column not in metadata_df.columns:
        raise ValueError(
            f"Ticker column '{ticker_column}' not found in metadata. "
            f"Available columns: {list(metadata_df.columns)}"
        )
    ticker_map: Dict[str, str] = {}
    for _, row in metadata_df.iterrows():
        series_code = row[MetadataColumns.SERIES_CODE]
        ticker = row.get(ticker_column)
        if pd.notna(ticker) and ticker:
            ticker_map[str(series_code)] = str(ticker)
    return ticker_map


def build_ticker_to_series_map(
    metadata_df: pd.DataFrame,
    ticker_source: TickerSource,
) -> Dict[str, str]:
    """Build ``ticker -> series_code`` map for a given vendor source."""
    series_to_ticker = build_series_to_ticker_map(metadata_df, ticker_source)
    return {ticker: series_code for series_code, ticker in series_to_ticker.items()}
