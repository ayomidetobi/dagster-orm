"""DataAPI class for semantic ORM layer."""

from typing import Any, Dict, List, Optional

import pandas as pd

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

        from duckdb_tinyorm_py import QueryBuilder

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

        # if field_type:
        #     query_filters[MetadataColumns.FIELD_TYPE] = (
        #         [field_type] if isinstance(field_type, str) else field_type
        #     )

        # if ticker_source:
        #     query_filters[MetadataColumns.TICKER_SOURCE] = [ticker_source.value]
        # else:
        #     query_filters[MetadataColumns.TICKER_SOURCE] = [TickerSource.BLOOMBERG.value]

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

        # if field_type:
        #     query_filters[MetadataColumns.FIELD_TYPE] = [field_type]

        # if ticker_source:
        #     query_filters[MetadataColumns.TICKER_SOURCE] = [ticker_source.value]
        # else:
        #     query_filters[MetadataColumns.TICKER_SOURCE] = [TickerSource.BLOOMBERG.value]

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

        Args:
            points: List of data point dicts with 'timestamp' and 'value' keys
            series_code: Series code identifier

        Returns:
            Prepared DataFrame or None if invalid
        """
        if not points:
            return None

        df = pd.DataFrame(points)
        if df.empty:
            return None

        required_columns = ["timestamp", "value"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return None

        df[ValueColumns.SERIES_CODE] = series_code
        df = df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]]

        if not pd.api.types.is_datetime64_any_dtype(df[ValueColumns.TIMESTAMP]):
            df[ValueColumns.TIMESTAMP] = pd.to_datetime(df[ValueColumns.TIMESTAMP])

        return df

    def _filter_existing_by_date_range(
        self, existing_df: pd.DataFrame, start_date: Any, end_date: Any
    ) -> pd.DataFrame:
        """Filter existing DataFrame to exclude rows in date range.

        Args:
            existing_df: DataFrame with existing data
            start_date: Start date (datetime or date string)
            end_date: End date (datetime or date string)

        Returns:
            Filtered DataFrame
        """
        existing_df[ValueColumns.TIMESTAMP] = pd.to_datetime(existing_df[ValueColumns.TIMESTAMP])

        start_date_dt = pd.to_datetime(start_date).date()
        end_date_dt = pd.to_datetime(end_date).date()

        existing_df["_date"] = existing_df[ValueColumns.TIMESTAMP].dt.date
        filtered_df = existing_df[
            ~((existing_df["_date"] >= start_date_dt) & (existing_df["_date"] <= end_date_dt))
        ]
        return filtered_df.drop(columns=["_date"])

    def _merge_and_deduplicate(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge existing and new data, deduplicate, and order by timestamp.

        Args:
            existing_df: DataFrame with existing data
            new_df: DataFrame with new data

        Returns:
            Merged, deduplicated, and ordered DataFrame
        """
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(
            subset=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP], keep="last"
        )
        return merged_df.sort_values(ValueColumns.TIMESTAMP)

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

        Args:
            data_points: Dict mapping series_code to list of data point dicts
                Each data point dict should have 'timestamp' and 'value' keys
            ticker_source: Ticker source (default: BLOOMBERG)
            force_refresh: If True, exclude existing data for date range before merging
            start_date: Start date for force_refresh exclusion (datetime or date string)
            end_date: End date for force_refresh exclusion (datetime or date string)

        Returns:
            Dict mapping series_code to S3 path where data was saved
        """
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
                final_df = new_df.sort_values(ValueColumns.TIMESTAMP)
            else:
                if force_refresh and start_date and end_date:
                    existing_df = self._filter_existing_by_date_range(
                        existing_df, start_date, end_date
                    )
                final_df = self._merge_and_deduplicate(existing_df, new_df)

            relative_path = build_s3_value_data_path(series_code, ticker_source)
            self.save_dataframe_to_s3(final_df, relative_path)
            saved_paths[series_code] = relative_path

        return saved_paths
