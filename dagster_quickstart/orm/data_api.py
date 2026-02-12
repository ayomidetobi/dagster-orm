"""DataAPI class for semantic ORM layer."""

from typing import Any, Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import ConnectionBindingError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.schema import TickerSource
from dagster_quickstart.orm.validation import MetadataValidator
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

        validator_metadata_repo = MetadataRepository(duckdb_repository, parquet_adapter, s3_adapter)
        self._validator = MetadataValidator(validator_metadata_repo)

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
            validator=self._validator,
        )

    def load_metadata_from_s3(self) -> pd.DataFrame:
        """Load metadata table from S3 Parquet file.

        Returns:
            DataFrame with metadata columns (validated against lookup tables)
        """
        metadata_df = self._metadata_repository.filter()
        validated_df = self._validator.validate_metadata_dataframe(metadata_df)
        return validated_df

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
