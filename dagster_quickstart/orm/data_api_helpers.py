"""Helper functions for DataAPI value data processing."""

from typing import Any, Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.schema import ValueColumns
from dagster_quickstart.utils.datetime_utils import (
    normalize_date_to_utc,
    normalize_pandas_timestamp_to_utc,
)


def prepare_new_dataframe(
    points: List[Dict[str, Any]], series_code: str, validation_repository: ValidationRepository
) -> Optional[pd.DataFrame]:
    """Prepare new data points as DataFrame with required columns.

    Ensures timestamps are timezone-aware and normalized to UTC.

    Args:
        points: List of data point dicts with 'timestamp' and 'value' keys
        series_code: Series code identifier
        validation_repository: ValidationRepository instance for validation

    Returns:
        Prepared DataFrame with UTC-normalized timestamps or None if invalid
    """
    if not points:
        return None

    required_columns = ["timestamp", "value"]
    if not validation_repository.validate_data_points_structure(points, required_columns):
        return None

    df = pd.DataFrame(points)
    if df.empty:
        return None

    df = df.copy()
    df[ValueColumns.SERIES_CODE] = series_code
    df = normalize_pandas_timestamp_to_utc(df, ValueColumns.TIMESTAMP)
    return df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]].copy()


def filter_existing_by_date_range(
    existing_df: pd.DataFrame, start_date: Any, end_date: Any
) -> pd.DataFrame:
    """Filter existing DataFrame to exclude rows in date range.

    Operates on a copy to prevent in-place mutation.
    Excludes rows where DATE(timestamp) BETWEEN start_date AND end_date (inclusive).

    Args:
        existing_df: DataFrame with existing data
        start_date: Start date (datetime or date string)
        end_date: End date (datetime or date string)

    Returns:
        Filtered DataFrame with UTC-normalized timestamps
    """
    df = existing_df.copy()
    df = normalize_pandas_timestamp_to_utc(df, ValueColumns.TIMESTAMP)

    # Normalize dates to UTC for comparison
    start_date_utc = normalize_date_to_utc(start_date)
    end_date_utc = normalize_date_to_utc(end_date)

    # Normalize timestamps to dates for comparison
    df_dates = df[ValueColumns.TIMESTAMP].dt.normalize()

    # Exclude rows where DATE(timestamp) BETWEEN start_date AND end_date (inclusive)
    mask = ~((df_dates >= start_date_utc) & (df_dates <= end_date_utc))
    filtered_df = df[mask].copy()

    # Ensure only required columns
    filtered_df = filtered_df[
        [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
    ].copy()

    return filtered_df


def merge_and_deduplicate(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    validation_repository: ValidationRepository,
) -> pd.DataFrame:
    """Merge existing and new data, deduplicate, and order by timestamp.

    Ensures required columns exist, prioritizes new data in deduplication,
    and returns a clean dataframe with reset index.

    Args:
        existing_df: DataFrame with existing data
        new_df: DataFrame with new data
        validation_repository: ValidationRepository instance for validation

    Returns:
        Merged, deduplicated, sorted DataFrame with reset index

    Raises:
        ValueError: If required columns are missing
    """
    # Verify required columns exist
    validation_repository.validate_value_dataframe_columns(existing_df, "existing_df")
    validation_repository.validate_value_dataframe_columns(new_df, "new_df")

    existing_df = existing_df.copy()
    new_df = new_df.copy()
    existing_df = normalize_pandas_timestamp_to_utc(existing_df, ValueColumns.TIMESTAMP)
    new_df = normalize_pandas_timestamp_to_utc(new_df, ValueColumns.TIMESTAMP)

    # Merge dataframes
    merged_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Deduplicate by (series_code, timestamp), prioritizing new rows (keep='last')
    merged_df = merged_df.drop_duplicates(
        subset=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP], keep="last"
    )

    # Sort by timestamp ascending
    merged_df = merged_df.sort_values(ValueColumns.TIMESTAMP, ascending=True)

    # Reset index and ensure only required columns
    required_cols = [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
    merged_df = merged_df[required_cols].reset_index(drop=True)

    return merged_df
