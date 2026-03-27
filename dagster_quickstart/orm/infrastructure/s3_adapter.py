"""S3 adapter for path resolution and URI construction.

This adapter provides S3 URI resolution and path construction logic.
It contains ZERO SQL and ZERO DuckDB calls.
"""

from dagster_quickstart.orm.s3_paths import (
    build_full_s3_uri,
    build_s3_control_table_path,
    build_s3_value_data_path,
    build_s3_wide_field_glob_relative,
    build_s3_wide_value_partition_path,
)
from dagster_quickstart.orm.schema import (
    S3_PARQUET_FILE_NAME,
    TableNames,
    TickerSource,
)


class S3Adapter:
    """Adapter for S3 path resolution and URI construction.

    Responsibilities:
    - Provide S3 URI resolution
    - Path construction logic
    - Bucket/prefix handling

    Must:
    - Contain ZERO SQL
    - Contain ZERO DuckDB calls
    - Only provide path/URI construction
    """

    def __init__(self, bucket: str):
        """Initialize S3 adapter with bucket name.

        Args:
            bucket: S3 bucket name
        """
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        """Get the S3 bucket name."""
        return self._bucket

    def get_metadata_uri(self, control_type: str = TableNames.METADATA) -> str:
        """Get full S3 URI for a metadata control table (or glob if ``TableNames.METADATA_WILDCARD``)."""
        relative_path = build_s3_control_table_path(control_type, S3_PARQUET_FILE_NAME)
        return build_full_s3_uri(relative_path, self._bucket)

    def get_lookup_uri(self) -> str:
        """Get full S3 URI for lookup table.

        Returns:
            Full S3 URI (e.g., 's3://bucket/control/lookup/data.parquet')
        """
        relative_path = build_s3_control_table_path("lookup", S3_PARQUET_FILE_NAME)
        return build_full_s3_uri(relative_path, self._bucket)

    def get_wide_value_partition_uri(
        self,
        field_type: str,
        year: int,
        month: int,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> str:
        """Full S3 URI for a wide-format monthly value partition."""
        relative_path = build_s3_wide_value_partition_path(
            field_type, year, month, tickersource, S3_PARQUET_FILE_NAME
        )
        return build_full_s3_uri(relative_path, self._bucket)

    def get_wide_field_glob_uri(
        self,
        field_type: str,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> str:
        """Full S3 URI glob for all monthly Parquet files under a vendor field partition."""
        relative_path = build_s3_wide_field_glob_relative(field_type, tickersource)
        return build_full_s3_uri(relative_path, self._bucket)

    def get_value_data_uri(
        self, series_code: str, tickersource: TickerSource = TickerSource.BLOOMBERG
    ) -> str:
        """Get full S3 URI for value data file.

        Args:
            series_code: Series code identifier
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)

        Returns:
            Full S3 URI (e.g., 's3://bucket/value-data/Bloomberg/series_code=AAPL_US_EQ/data.parquet')
        """
        relative_path = build_s3_value_data_path(series_code, tickersource, S3_PARQUET_FILE_NAME)
        return build_full_s3_uri(relative_path, self._bucket)

    def get_relative_path_uri(self, relative_path: str) -> str:
        """Get full S3 URI from relative path.

        Args:
            relative_path: Relative S3 path (without bucket)

        Returns:
            Full S3 URI
        """
        return build_full_s3_uri(relative_path, self._bucket)
