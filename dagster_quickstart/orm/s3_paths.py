"""S3 path building functions for control tables and value data."""

from dagster_quickstart.orm.schema import (
    S3_BASE_PATH_CONTROL,
    S3_BASE_PATH_VALUE_DATA,
    S3_PARQUET_FILE_NAME,
    TickerSource,
)
from dagster_quickstart.resources.duckdb_datacacher import join_s3


def build_s3_control_table_path(control_type: str, filename: str = S3_PARQUET_FILE_NAME) -> str:
    """Build relative S3 path for control table Parquet file.

    Control tables are the system of record for lookup tables and metadata_series.
    They are versioned by run date (YYYY-MM-DD) and are immutable.

    Args:
        control_type: Type of control table ('lookup', 'metadata_series', 'field_map')
        filename: Parquet filename (default: uses S3_PARQUET_FILE_NAME constant)

    Returns:
        Relative S3 path (e.g., 'control/lookup/data.parquet')
    """
    return f"{S3_BASE_PATH_CONTROL}/{control_type}/{filename}"


def build_s3_value_data_path(
    series_code: str,
    tickersource: TickerSource = TickerSource.BLOOMBERG,
    filename: str = S3_PARQUET_FILE_NAME,
) -> str:
    """Build relative S3 path for unified value data Parquet file.

    Value data is stored in a single file per series_code, ordered by timestamp.
    Path format: value-data/ticker_source={tickersource}/series_code={series_code}/data.parquet

    Args:
        series_code: Series code (readable identifier)
        tickersource: Ticker source (default: TickerSource.BLOOMBERG)
        filename: Parquet filename (default: uses S3_PARQUET_FILE_NAME constant)

    Returns:
        Relative S3 path (e.g., 'value-data/ticker_source=Bloomberg/series_code=AAPL_US_EQ/data.parquet')
    """
    return f"{S3_BASE_PATH_VALUE_DATA}/{tickersource.value}/{series_code}/{filename}"


def build_full_s3_uri(relative_path: str, bucket: str) -> str:
    """Build full S3 URI from relative path and bucket.

    Args:
        relative_path: Relative S3 path
        bucket: S3 bucket name

    Returns:
        Full S3 URI (e.g., 's3://bucket/control/lookup/data.parquet')
    """
    return join_s3(bucket, relative_path)
