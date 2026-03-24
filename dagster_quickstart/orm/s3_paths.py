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


def build_s3_wide_value_partition_path(
    field_type: str,
    year: int,
    month: int,
    tickersource: TickerSource = TickerSource.BLOOMBERG,
    filename: str = S3_PARQUET_FILE_NAME,
) -> str:
    """Relative S3 path for wide-format value Parquet (hive-style year/month).

    One file per (ticker source, field_type, year, month) with rows=timestamp,
    columns=series_code. Does not partition by series_code.

    Args:
        field_type: Bloomberg field / partition key (e.g. PX_LAST).
        year: Calendar year.
        month: Calendar month (1-12).
        tickersource: Data vendor / ticker source.
        filename: Parquet file name.

    Returns:
        Relative path under the bucket.
    """
    return (
        f"{S3_BASE_PATH_VALUE_DATA}/wide/{tickersource.value}/"
        f"field_type={field_type}/year={year:04d}/month={month:02d}/{filename}"
    )


def build_s3_wide_field_glob_relative(
    field_type: str,
    tickersource: TickerSource = TickerSource.BLOOMBERG,
    filename: str = S3_PARQUET_FILE_NAME,
) -> str:
    """Relative path glob under one vendor field (all year/month Parquet files)."""
    return (
        f"{S3_BASE_PATH_VALUE_DATA}/wide/{tickersource.value}/"
        f"field_type={field_type}/**/{filename}"
    )


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
