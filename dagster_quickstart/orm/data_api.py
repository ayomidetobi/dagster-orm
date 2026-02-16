"""DataAPI class for semantic ORM layer."""

from typing import Any, Dict, List, Optional

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import ConnectionBindingError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.s3_paths import build_s3_value_data_path
from dagster_quickstart.orm.schema import MetadataColumns, TickerSource, ValueColumns
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.utils.datetime_utils import (
    normalize_date_to_utc,
    normalize_pandas_timestamp_to_utc,
)


class DataAPI:
    """Semantic ORM API for querying metadata and value data.

    Provides a high-level interface for filtering metadata and retrieving
    corresponding value data from DuckDB with S3 as the datalake.

    Example:
        duckdb_resource = context.resources.duckdb
        data_api = DataAPI(duckdb_resource)

        dataset = data_api.get(asset_class=["fx", "comdty"], country=["usa"])
        metadata_df = dataset.info()
        values_df = dataset.value(ValueQueryParams(start="2024-01-01", end="2024-12-31"))
    """

    def __init__(self, duckdb_resource: DuckDBResource):
        """Initialize DataAPI with DuckDB resource.

        Sets up dependency injection: connection -> DuckDbRepository -> repositories -> QuerySet

        Args:
            duckdb_resource: DuckDBResource instance with connection and S3 access configured

        Raises:
            ConnectionBindingError: If duckdb_resource is None or invalid
        """
        if duckdb_resource is None:
            raise ConnectionBindingError("DuckDB resource cannot be None")

        if not hasattr(duckdb_resource, "_con"):
            raise ConnectionBindingError(
                "DuckDB resource must have a connection. Ensure setup_for_execution() was called."
            )

        self._duckdb_resource = duckdb_resource
        connection = duckdb_resource._con
        bucket = duckdb_resource.get_bucket()

        duckdb_repository = DuckDbRepository(connection)
        parquet_adapter = ParquetAdapter()
        s3_adapter = S3Adapter(bucket)
        temp_table_manager = TempTableManager(duckdb_repository)

        self._metadata_repository = MetadataRepository(
            duckdb_repository, parquet_adapter, s3_adapter
        )
        self._value_repository = ValueRepository(duckdb_repository, parquet_adapter, s3_adapter)
        self._temp_table_manager = temp_table_manager

        # Create ValidationRepository for wide-format lookup table validation
        self._validation_repository = ValidationRepository(
            duckdb_repository, parquet_adapter, s3_adapter, temp_table_manager
        )

    def get(self, **filters: Any) -> QuerySet:
        """Create QuerySet with metadata filters.

        Args:
            **filters: Keyword arguments mapping metadata column names to filter values.
                Values can be single strings or lists of strings.
                Example: asset_class=["fx", "comdty"], country=["usa"]

        Returns:
            QuerySet instance configured with the provided filters

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
        """
        normalized_filters: Dict[str, List[str]] = {}

        for filter_field, filter_value in filters.items():
            if isinstance(filter_value, str):
                normalized_filters[filter_field] = [filter_value]
            elif isinstance(filter_value, list):
                normalized_filters[filter_field] = filter_value
            else:
                normalized_filters[filter_field] = [str(filter_value)]

        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=normalized_filters,
            validation_repository=self._validation_repository,
        )

    def load_metadata_from_s3(self) -> pd.DataFrame:
        """Load metadata table from S3 Parquet file.

        Returns:
            DataFrame with metadata columns (validated against lookup tables)
        """
        return self._validation_repository.filter_with_validation(filters=None)

    def load_lookup_table_from_s3(self) -> pd.DataFrame:
        """Load lookup table from S3 Parquet file.

        Returns:
            DataFrame with lookup table columns (lookup_type, code, name)
        """
        lookup_uri = self._metadata_repository._s3_adapter.get_lookup_uri()
        query_builder, param_values = self._metadata_repository._build_filtered_query(None)
        adapted_sql, builder_params = (
            self._metadata_repository._parquet_adapter.adapt_query_builder_for_parquet(
                query_builder, lookup_uri
            )
        )
        all_params = param_values + builder_params
        if all_params:
            return self._metadata_repository._repository.execute_raw_sql(adapted_sql, all_params)
        return self._metadata_repository._repository.execute_raw_sql(adapted_sql)

    def load_value_data_from_s3(
        self,
        series_code: str,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Load value data for a specific series_code from S3 Parquet file.

        Args:
            series_code: Series code identifier
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)

        Returns:
            DataFrame with series_code, timestamp, and value columns
        """
        return self._value_repository.get_series_data(series_code, tickersource)

    def save_dataframe_to_s3(
        self,
        dataframe: pd.DataFrame,
        relative_path: str,
    ) -> None:
        """Save DataFrame to S3 as Parquet file.

        Args:
            dataframe: DataFrame to save
            relative_path: Relative S3 path (without bucket)
        """
        temp_table_name = self._temp_table_manager.create_temp_table_from_dataframe(dataframe)
        full_uri = self._metadata_repository._s3_adapter.get_relative_path_uri(relative_path)

        query_builder = QueryBuilder(temp_table_name)
        self._metadata_repository._repository.copy_builder_to_parquet(query_builder, full_uri)
        self._temp_table_manager.drop_temp_table(temp_table_name)

    def get_or_create_temp_table(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        force_recreate: bool = False,
    ) -> str:
        """Get existing temp table or create a new one if it doesn't exist.

        Args:
            dataframe: DataFrame to convert to temp table (only used if creating new)
            table_name: Name of the table (required for reuse)
            force_recreate: If True, drop and recreate even if table exists

        Returns:
            Name of the temp table (same as input table_name)
        """
        return self._temp_table_manager.get_or_create_temp_table(
            dataframe, table_name, force_recreate
        )

    def create_temp_table_from_dataframe(
        self, dataframe: pd.DataFrame, table_name: Optional[str] = None
    ) -> str:
        """Create temporary table from DataFrame.

        Args:
            dataframe: DataFrame to convert to temp table
            table_name: Optional table name (generated if not provided)

        Returns:
            Name of the created temporary table
        """
        return self._temp_table_manager.create_temp_table_from_dataframe(dataframe, table_name)

    def temp_table_exists(self, table_name: str) -> bool:
        """Check if a temporary table exists in the registry.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists in registry, False otherwise
        """
        return self._temp_table_manager.temp_table_exists(table_name)

    def get_temp_table_name(self, table_name: str) -> Optional[str]:
        """Get temp table name if it exists in registry.

        Args:
            table_name: Name of the table to get

        Returns:
            Table name if exists, None otherwise
        """
        return self._temp_table_manager.get_temp_table_name(table_name)

    def create_temp_table_from_csv(
        self, csv_path: str, table_name: str, force_recreate: bool = False
    ) -> str:
        """Create temporary table from CSV file using DuckDB read_csv_auto.

        Args:
            csv_path: Path to the CSV file to load
            table_name: Name of the temp table to create
            force_recreate: If True, drop and recreate even if table exists

        Returns:
            Name of the created temp table
        """
        return self._temp_table_manager.create_temp_table_from_csv(
            csv_path, table_name, force_recreate
        )

    def drop_temp_table(self, table_name: str) -> None:
        """Drop temporary table and remove from registry.

        Args:
            table_name: Name of temporary table to drop
        """
        self._temp_table_manager.drop_temp_table(table_name)

    def get_series_codes(
        self,
        field_type: Optional[str] = None,
        ticker_source: Optional[TickerSource] = None,
        **filters: Any,
    ) -> List[str]:
        """Get list of series codes from metadata.

        Args:
            field_type: Optional field_type filter
            ticker_source: Optional ticker_source filter (default: BLOOMBERG)
            **filters: Additional metadata filters

        Returns:
            List of series code strings
        """
        query_filters: Dict[str, List[str]] = {}

        for filter_field, filter_value in filters.items():
            if isinstance(filter_value, str):
                query_filters[filter_field] = [filter_value]
            elif isinstance(filter_value, list):
                query_filters[filter_field] = filter_value
            else:
                query_filters[filter_field] = [str(filter_value)]

        metadata_df = self.get(**query_filters).info()

        if metadata_df.empty:
            return []

        return metadata_df[MetadataColumns.SERIES_CODE].unique().tolist()

    def get_tickers(
        self,
        series_codes: List[str],
        field_type: Optional[str] = None,
        ticker_source: Optional[TickerSource] = None,
    ) -> Dict[str, str]:
        """Get ticker mapping for series codes.

        Args:
            series_codes: List of series codes to get tickers for
            field_type: Optional field_type filter
            ticker_source: Optional ticker_source filter (default: BLOOMBERG)

        Returns:
            Dict mapping series_code to ticker
        """
        if not series_codes:
            return {}

        query_filters: Dict[str, List[str]] = {
            MetadataColumns.SERIES_CODE: series_codes,
        }

        metadata_df = self.get(**query_filters).info()

        if metadata_df.empty:
            return {}

        ticker_map = {}
        for _, row in metadata_df.iterrows():
            series_code = row[MetadataColumns.SERIES_CODE]
            ticker = row[MetadataColumns.TICKER]
            if pd.notna(ticker) and ticker:
                ticker_map[series_code] = str(ticker)

        return ticker_map


    def _prepare_new_dataframe(
        self, points: List[Dict[str, Any]], series_code: str
    ) -> Optional[pd.DataFrame]:
        """Prepare new data points as DataFrame with required columns.

        Ensures timestamps are timezone-aware and normalized to UTC.

        Args:
            points: List of data point dicts with 'timestamp' and 'value' keys
            series_code: Series code identifier

        Returns:
            Prepared DataFrame with UTC-normalized timestamps or None if invalid
        """
        if not points:
            return None

        required_columns = ["timestamp", "value"]
        if not self._validation_repository.validate_data_points_structure(
            points, required_columns
        ):
            return None

        df = pd.DataFrame(points)
        if df.empty:
            return None

        df = df.copy()
        df[ValueColumns.SERIES_CODE] = series_code
        df = normalize_pandas_timestamp_to_utc(df, ValueColumns.TIMESTAMP)
        return df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]].copy()

    def _filter_existing_by_date_range(
        self, existing_df: pd.DataFrame, start_date: Any, end_date: Any
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
        filtered_df = filtered_df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]].copy()

        return filtered_df

    def _merge_and_deduplicate(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge existing and new data, deduplicate, and order by timestamp.

        Ensures required columns exist, prioritizes new data in deduplication,
        and returns a clean dataframe with reset index.

        Args:
            existing_df: DataFrame with existing data
            new_df: DataFrame with new data

        Returns:
            Merged, deduplicated, sorted DataFrame with reset index

        Raises:
            ValueError: If required columns are missing
        """
        # Verify required columns exist
        self._validation_repository.validate_value_dataframe_columns(existing_df, "existing_df")
        self._validation_repository.validate_value_dataframe_columns(new_df, "new_df")

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

    def check_data_exists_for_date_range(
        self,
        series_codes: List[str],
        start_date: Any,
        end_date: Any,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
    ) -> Dict[str, bool]:
        """Check if data already exists for given series codes in date range.

        Args:
            series_codes: List of series codes to check
            start_date: Start date (datetime or date string)
            end_date: End date (datetime or date string)
            ticker_source: Ticker source (default: BLOOMBERG)

        Returns:
            Dict mapping series_code to bool indicating if data exists
        """
        start_date_utc = normalize_date_to_utc(start_date)
        end_date_utc = normalize_date_to_utc(end_date)

        result: Dict[str, bool] = {}

        for series_code in series_codes:
            existing_df = self._value_repository.get_series_data(
                series_code=series_code,
                tickersource=ticker_source,
            )

            if existing_df.empty:
                result[series_code] = False
                continue

            existing_df = existing_df.copy()
            existing_df = normalize_pandas_timestamp_to_utc(existing_df, ValueColumns.TIMESTAMP)

            existing_dates = existing_df[ValueColumns.TIMESTAMP].dt.normalize()
            has_overlap = (
                (existing_dates >= start_date_utc) & (existing_dates <= end_date_utc)
            ).any()

            result[series_code] = has_overlap

        return result

    def save_value_data_to_s3(
        self,
        data_points: Dict[str, List[Dict[str, Any]]],
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
        force_refresh: bool = False,
        start_date: Optional[Any] = None,
        end_date: Optional[Any] = None,
    ) -> Dict[str, str]:
        """Save value data points to S3 for multiple series with merge logic.

        Handles existing files by:
        - If no existing file: Write new data ordered by timestamp
        - If existing file AND force_refresh=False: Load all existing, merge with new,
          deduplicate by (series_code, timestamp), prioritize new rows, order by timestamp
        - If existing file AND force_refresh=True: Load existing but exclude rows where
          DATE(timestamp) BETWEEN start_date AND end_date, merge with new, deduplicate,
          order by timestamp

        All timestamps are normalized to UTC before processing.

        Args:
            data_points: Dict mapping series_code to list of data point dicts
                Each data point dict should have 'timestamp' and 'value' keys
            ticker_source: Ticker source (default: BLOOMBERG)
            force_refresh: If True, exclude existing data for date range before merging
            start_date: Start date for force_refresh exclusion (datetime or date string)
            end_date: End date for force_refresh exclusion (datetime or date string)

        Returns:
            Dict mapping series_code to S3 path where data was saved

        Raises:
            ValueError: If force_refresh=True but start_date or end_date is missing,
                or if start_date > end_date
        """
        # Validate date parameters when force_refresh is True
        self._validation_repository.validate_date_range_for_force_refresh(
            force_refresh, start_date, end_date
        )

        saved_paths: Dict[str, str] = {}

        if not data_points:
            return saved_paths

        for series_code, points in data_points.items():
            new_df = self._prepare_new_dataframe(points, series_code)
            if new_df is None:
                continue

            existing_df = self._value_repository.get_series_data(
                series_code=series_code,
                tickersource=ticker_source,
            )

            if existing_df.empty:
                # No existing data: use new data, ensure sorted and UTC-normalized
                final_df = new_df.sort_values(ValueColumns.TIMESTAMP, ascending=True).reset_index(
                    drop=True
                )
            else:
                existing_df = existing_df.copy()
                existing_df = normalize_pandas_timestamp_to_utc(existing_df, ValueColumns.TIMESTAMP)

                # Ensure existing_df has required columns
                if ValueColumns.SERIES_CODE not in existing_df.columns:
                    existing_df[ValueColumns.SERIES_CODE] = series_code
                existing_df = existing_df[
                    [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
                ].copy()

                if force_refresh and start_date and end_date:
                    existing_df = self._filter_existing_by_date_range(
                        existing_df, start_date, end_date
                    )

                final_df = self._merge_and_deduplicate(existing_df, new_df)

            # Ensure final dataframe has consistent dtypes and only required columns
            final_df = final_df[
                [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            ].copy()
            final_df = final_df.sort_values(ValueColumns.TIMESTAMP, ascending=True).reset_index(
                drop=True
            )

            relative_path = build_s3_value_data_path(series_code, ticker_source)
            self.save_dataframe_to_s3(final_df, relative_path)
            saved_paths[series_code] = relative_path

        return saved_paths
