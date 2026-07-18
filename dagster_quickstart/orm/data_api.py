"""DataAPI class for semantic ORM layer."""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Unpack, Union

import pandas as pd
from decouple import config
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import ConnectionBindingError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.option_utils import dataframe_filter_options
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.s3_paths import build_s3_wide_value_partition_path
from dagster_quickstart.orm.schema import (
    FilterParams,
    MetadataColumns,
    TableNames,
    TickerSource,
    ValueColumns,
    get_vendor_field_column,
    ticker_source_uses_wide_storage,
)
from dagster_quickstart.orm.storage.wide_partition import (
    merge_wide_monthly_partition,
    sanitize_wide_numeric_columns,
    slice_wide_for_calendar_month,
    wide_frame_covers_utc_dates,
)
from dagster_quickstart.orm.ticker_mapping import build_series_to_ticker_map
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.utils.datetime_utils import (
    dates_by_year_month,
    iter_year_months,
    normalize_date_to_utc,
    normalize_pandas_timestamp_to_utc,
)


def _metadata_vendor_field_column(ticker_source: TickerSource) -> str:
    """Metadata Parquet column for vendor field code (e.g. PX_LAST, YIELD)."""
    return get_vendor_field_column(ticker_source)


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

    def __init__(
        self,
        duckdb_resource: Optional[DuckDBResource] = None,
        out_of_cache: bool = False,
    ):
        """Initialize DataAPI with DuckDB resource.

        Sets up dependency injection: connection -> DuckDbRepository -> repositories -> QuerySet

        Args:
            duckdb_resource: DuckDBResource instance with connection and S3 access configured
            out_of_cache: Default ``out_of_cache`` behavior for QuerySets created from
                this DataAPI instance. QuerySet value methods may still override it
                explicitly per call.

        Raises:
            ConnectionBindingError: If duckdb_resource is invalid or cannot be created
        """
        # If no resource is provided, try to construct one from environment variables.
        if duckdb_resource is None:
            bucket = config("S3_BUCKET", default=None)
            access_key = config("S3_ACCESS_KEY", default=None)
            secret_key = config("S3_SECRET_KEY", default=None)
            region = config("S3_REGION", default=None)

            if not all([bucket, access_key, secret_key, region]):
                raise ConnectionBindingError(
                    "DuckDB resource not provided and S3 configuration is incomplete. "
                    "Either pass an explicit DuckDBResource or set S3_BUCKET, "
                    "S3_ACCESS_KEY, S3_SECRET_KEY and S3_REGION environment variables."
                )

            duckdb_cacher = duckdb_datacacher(
                bucket=bucket,
                access_key=access_key,
                secret_key=secret_key,
                region=region,
            )
            duckdb_resource = DuckDBResource(cacher=duckdb_cacher)
            # setup_for_execution only needs to bind the connection; context is unused here.
            duckdb_resource.setup_for_execution(None)

        if not hasattr(duckdb_resource, "_con"):
            raise ConnectionBindingError(
                "DuckDB resource must have a connection. Ensure setup_for_execution() was called."
            )

        self._duckdb_resource = duckdb_resource
        self._out_of_cache = out_of_cache
        connection = duckdb_resource._con
        bucket = duckdb_resource.get_bucket()

        duckdb_repository = DuckDbRepository(connection)
        parquet_adapter = ParquetAdapter()
        s3_adapter = S3Adapter(bucket)
        temp_table_manager = TempTableManager(duckdb_repository)

        self._metadata_repository = MetadataRepository(
            duckdb_repository, parquet_adapter, s3_adapter
        )
        self._temp_table_manager = temp_table_manager

        self._validation_repository = ValidationRepository(
            duckdb_repository, parquet_adapter, s3_adapter, temp_table_manager
        )
        self._value_repository = ValueRepository(
            duckdb_repository,
            parquet_adapter,
            s3_adapter,
            validation_repository=self._validation_repository,
            metadata_repository=self._metadata_repository,
        )

    def get(self, **filters: Unpack[FilterParams]) -> QuerySet:
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
        control_table, normalized_filters = self._normalize_query_filters(filters)

        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=normalized_filters,
            validation_repository=self._validation_repository,
            exclude=False,
            out_of_cache=self._out_of_cache,
            control_table=control_table,
        )

    def _normalize_query_filters(
        self, filters: Dict[str, Any]
    ) -> Tuple[Optional[str], Dict[str, List[str]]]:
        """Normalize filter input for QuerySet construction."""
        filters = dict(filters)
        control_table = filters.pop("control_table", None)
        effective_control = control_table or TableNames.METADATA_WILDCARD
        if effective_control in (TableNames.METADATA_WILDCARD, TableNames.METADATA_DERIVED):
            if "field_type" in filters and MetadataColumns.CALC_TYPE not in filters:
                filters[MetadataColumns.CALC_TYPE] = filters.pop("field_type")

        normalized_filters: Dict[str, List[str]] = {}
        for filter_field, filter_value in filters.items():
            if isinstance(filter_value, str):
                normalized_filters[filter_field] = [filter_value]
            elif isinstance(filter_value, list):
                normalized_filters[filter_field] = filter_value
            else:
                normalized_filters[filter_field] = [str(filter_value)]

        return control_table, normalized_filters

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

    def filter_options(
        self,
        fields: Optional[Union[str, List[str]]] = None,
        *,
        as_dataframe: bool = False,
    ) -> Union[List[str], Dict[str, List[str]], pd.DataFrame]:
        """Return global lookup-based filter options.

        Unlike :meth:`QuerySet.filter_options`, this method is not contextual to a
        current query. It reads the lookup table and returns options available
        globally across the catalog.

        Args:
            fields: Lookup field name, list of field names, or ``None`` for all fields.
            as_dataframe: When ``True``, return a normalized ``field`` / ``value`` DataFrame.

        Returns:
            Lookup-derived filter options for the requested fields.
        """
        lookup_df = self.load_lookup_table_from_s3()
        return dataframe_filter_options(lookup_df, fields=fields, as_dataframe=as_dataframe)

    def load_value_data_from_s3(
        self,
        series_code: str,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Load value data for a series from wide monthly Parquet partitions.

        Returns:
            DataFrame with series_code, timestamp, and value columns
        """
        return self._value_repository.get_series_data(series_code, tickersource)

    def save_dataframe_to_s3(
        self,
        dataframe: pd.DataFrame,
        relative_path: str,
        parquet_compression: Optional[str] = None,
    ) -> None:
        """Save DataFrame to S3 as Parquet file.

        Args:
            dataframe: DataFrame to save
            relative_path: Relative S3 path (without bucket)
            parquet_compression: Optional Parquet codec (e.g. zstd, snappy) for COPY.
        """
        temp_table_name = self._temp_table_manager.create_temp_table_from_dataframe(dataframe)
        full_uri = self._metadata_repository._s3_adapter.get_relative_path_uri(relative_path)

        query_builder = QueryBuilder(temp_table_name)
        self._metadata_repository._repository.copy_builder_to_parquet(
            query_builder, full_uri, compression=parquet_compression
        )
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

    def validate_date_range_for_force_refresh(
        self, force_refresh: bool, start_date: Any, end_date: Any
    ) -> None:
        """Validate start/end when ``force_refresh`` is enabled."""
        self._validation_repository.validate_date_range_for_force_refresh(
            force_refresh, start_date, end_date
        )

    def get_series_codes(
        self,
        field_type: Optional[str] = None,
        ticker_source: Optional[TickerSource] = None,
        **filters: Unpack[FilterParams],
    ) -> List[str]:
        """Get list of series codes from metadata.

        Args:
            field_type: Optional vendor field code (e.g. PX_LAST). Filter uses ``bbg_field`` when
                ``ticker_source`` is Bloomberg (default), or ``mds_field`` when MDS.
            ticker_source: Selects which vendor field column applies to ``field_type`` (Bloomberg vs
                MDS). Metadata Parquet has no ``ticker_source`` column, so this does not add a SQL
                filter—only ``bbg_field`` / ``mds_field`` does.
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

        if field_type:
            source_for_field = (
                ticker_source if ticker_source is not None else TickerSource.BLOOMBERG
            )
            field_col = _metadata_vendor_field_column(source_for_field)
            query_filters[field_col] = [field_type]

        metadata_df = self.get(control_table=TableNames.METADATA, **query_filters).info()

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
            field_type: Optional vendor field code; matched on ``bbg_field`` or ``mds_field`` per
                ``ticker_source`` (default Bloomberg).
            ticker_source: Which vendor ticker column to read (``bbg_ticker`` vs ``mds_ticker``).

        Returns:
            Dict mapping series_code to ticker

        Raises:
            ValueError: If ticker_source is not supported or corresponding ticker column doesn't exist
        """
        if not series_codes:
            return {}

        # Default to BLOOMBERG if not specified
        if ticker_source is None:
            ticker_source = TickerSource.BLOOMBERG

        query_filters: Dict[str, List[str]] = {
            MetadataColumns.SERIES_CODE: series_codes,
        }

        if field_type:
            field_col = _metadata_vendor_field_column(ticker_source)
            query_filters[field_col] = [field_type]

        metadata_df = self.get(control_table=TableNames.METADATA, **query_filters).info()

        if metadata_df.empty:
            return {}

        return build_series_to_ticker_map(metadata_df, ticker_source)

    def get_excluding(self, **filters: Unpack[FilterParams]) -> QuerySet:
        """Create QuerySet with inverted metadata filters (exclude matching values).

        Args:
            **filters: Same as :meth:`get`, including optional ``control_table``.
                Example: ``country=["usa"]`` excludes rows where country is USA.

        Returns:
            QuerySet instance configured with inverted filters

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
        """
        control_table, normalized_filters = self._normalize_query_filters(filters)

        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=normalized_filters,
            validation_repository=self._validation_repository,
            exclude=True,
            out_of_cache=self._out_of_cache,
            control_table=control_table,
        )

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
        if not ticker_source_uses_wide_storage(ticker_source):
            return {sc: False for sc in series_codes}

        field_map = self._value_repository._resolve_vendor_field_map(series_codes, ticker_source)
        by_field: Dict[str, List[str]] = defaultdict(list)
        for sc in series_codes:
            vf = field_map.get(sc)
            if vf:
                by_field[vf].append(sc)
        result: Dict[str, bool] = {}
        for vf, codes in by_field.items():
            sub = self.check_wide_data_exists_for_date_range(
                series_codes=codes,
                start_date=start_date,
                end_date=end_date,
                field_type=vf,
                ticker_source=ticker_source,
            )
            result.update(sub)
        for sc in series_codes:
            if sc not in result:
                result[sc] = False
        return result

    def read_wide_value_partition(
        self,
        field_type: str,
        year: int,
        month: int,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Load a monthly wide Parquet partition as a DataFrame (timestamp index, series columns)."""
        relative_path = build_s3_wide_value_partition_path(field_type, year, month, ticker_source)
        uri = self._metadata_repository._s3_adapter.get_relative_path_uri(relative_path)
        esc = uri.replace("'", "''")
        sql = f"SELECT * FROM read_parquet('{esc}')"
        try:
            df = self._metadata_repository._repository.execute_raw_sql(sql)
        except Exception:
            return pd.DataFrame()
        if df.empty or ValueColumns.TIMESTAMP not in df.columns:
            return pd.DataFrame()
        df = normalize_pandas_timestamp_to_utc(df, ValueColumns.TIMESTAMP)
        return df.set_index(ValueColumns.TIMESTAMP).sort_index()

    def write_wide_value_partition(
        self,
        wide_df: pd.DataFrame,
        field_type: str,
        year: int,
        month: int,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
        parquet_compression: str = "zstd",
    ) -> str:
        """Write one wide monthly partition (overwrites object). Returns relative S3 path."""
        relative_path = build_s3_wide_value_partition_path(field_type, year, month, ticker_source)
        if wide_df.empty:
            return relative_path

        out = wide_df.sort_index()
        if out.index.name != ValueColumns.TIMESTAMP:
            if ValueColumns.TIMESTAMP in out.columns:
                out = out.set_index(ValueColumns.TIMESTAMP)
            else:
                raise ValueError("wide_df must have timestamp index or column")
        out = out.reset_index()
        out = normalize_pandas_timestamp_to_utc(out, ValueColumns.TIMESTAMP)
        self.save_dataframe_to_s3(
            out,
            relative_path,
            parquet_compression=parquet_compression,
        )
        return relative_path

    def check_wide_data_exists_for_date_range(
        self,
        series_codes: List[str],
        start_date: Any,
        end_date: Any,
        field_type: str,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
    ) -> Dict[str, bool]:
        """Per-series coverage: True only if all UTC dates in range are non-null in wide partitions."""
        if not series_codes:
            return {}
        start_date_utc = normalize_date_to_utc(start_date)
        end_date_utc = normalize_date_to_utc(end_date)
        by_month = dates_by_year_month(start_date_utc.to_pydatetime(), end_date_utc.to_pydatetime())
        result: Dict[str, bool] = {sc: True for sc in series_codes}
        for (year, month), dlist in by_month.items():
            wide = self.read_wide_value_partition(field_type, year, month, ticker_source)
            for sc in series_codes:
                if not result[sc]:
                    continue
                if not wide_frame_covers_utc_dates(wide, sc, dlist):
                    result[sc] = False
        return result

    def write_wide_value_partitions(
        self,
        wide_df: pd.DataFrame,
        field_type: str,
        ticker_source: TickerSource,
        start_date: Any,
        end_date: Any,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        """Merge and write monthly wide Parquet partitions for the given date span.

        Args:
            wide_df: Wide frame (DatetimeIndex UTC, one column per series_code).
            field_type: Vendor field partition (e.g. PX_LAST, or DERIVED for internal series).
            ticker_source: Bloomberg, MDS, or Internal.
            start_date, end_date: Span used to iterate months and optional strip range.
            force_refresh: When True, strip existing rows in [start_date, end_date] before merge.

        Returns:
            Dict with partitions_written, row_count_max, column_count, written_relative_paths.
        """
        if wide_df.empty:
            return {
                "partitions_written": 0,
                "row_count_max": 0,
                "column_count": 0,
                "written_relative_paths": [],
                "partition_errors": [],
            }

        wide = sanitize_wide_numeric_columns(wide_df.sort_index())
        strip_range: Optional[Tuple[Any, Any]] = None
        if force_refresh:
            strip_range = (
                normalize_date_to_utc(start_date).to_pydatetime(),
                normalize_date_to_utc(end_date).to_pydatetime(),
            )

        written_paths: List[str] = []
        partition_errors: List[Dict[str, Any]] = []
        row_max = 0
        logger = logging.getLogger(__name__)
        for year, month in iter_year_months(start_date, end_date):
            try:
                inc = slice_wide_for_calendar_month(wide, year, month)
                if inc.empty:
                    continue
                existing = self.read_wide_value_partition(field_type, year, month, ticker_source)
                merged = merge_wide_monthly_partition(existing, inc, strip_range)
                if merged.empty:
                    continue
                rel = self.write_wide_value_partition(
                    merged, field_type, year, month, ticker_source
                )
                written_paths.append(rel)
                row_max = max(row_max, len(merged))
            except Exception as exc:
                error_info = {
                    "year": year,
                    "month": month,
                    "field_type": field_type,
                    "error": str(exc),
                }
                partition_errors.append(error_info)
                logger.warning(
                    "Failed to write wide partition year=%s month=%s field_type=%s: %s",
                    year,
                    month,
                    field_type,
                    exc,
                    exc_info=True,
                )

        return {
            "partitions_written": len(written_paths),
            "row_count_max": row_max,
            "column_count": int(wide.shape[1]),
            "written_relative_paths": written_paths,
            "partition_errors": partition_errors,
        }


from dagster_quickstart.orm.fx import FX
