"""DuckDB IO Manager for ORM layer.

Uses DataAPI and ORM layer for all S3 Parquet operations.
No raw SQL - all queries go through the semantic ORM layer.
"""

from typing import Any

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    get_dagster_logger,
    io_manager,
)

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.s3_paths import build_s3_value_data_path
from dagster_quickstart.orm.schema import (
    S3_BASE_PATH_VALUE_DATA,
    TickerSource,
)
from dagster_quickstart.resources import DuckDBResource

logger = get_dagster_logger()


class DuckDBIOManager(ConfigurableIOManager):
    """IO Manager for storing and loading data to/from S3 Parquet files via ORM.

    Uses DataAPI for S3 Parquet read/write operations.
    Integrates with the ORM layer for consistent data handling.
    No raw SQL - all queries go through the semantic ORM layer.
    """

    duckdb: DuckDBResource
    s3_base_path: str = S3_BASE_PATH_VALUE_DATA

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output from assets - save data to S3 Parquet files.

        Args:
            context: Dagster output context
            obj: Data to save (DataFrame, Series, or list)
        """
        if obj is None:
            return

        # Convert to pandas DataFrame if needed
        df = self._convert_to_dataframe(obj, context)

        if df.empty:
            context.log.warning(f"No data to save for asset {context.asset_key}")
            return

        # Determine S3 path based on asset metadata or asset key
        relative_path = self._resolve_s3_path(context, df)

        # Save using DataAPI (ORM layer)
        data_api = DataAPI(self.duckdb)
        data_api.save_dataframe_to_s3(df, relative_path)

        context.log.info(f"Saved {len(df)} rows to S3: {relative_path}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from S3 Parquet files using ORM layer.

        Uses DataAPI.load_value_data_from_s3() for semantic querying with time filters.
        No raw SQL - all queries go through the ORM layer.

        Args:
            context: Input context

        Returns:
            DataFrame with loaded data
        """
        data_api = DataAPI(self.duckdb)

        # Extract series_code and tickersource from metadata
        series_code = None
        tickersource = TickerSource.BLOOMBERG

        if context.metadata:
            series_code = context.metadata.get("series_code")
            tickersource_str = context.metadata.get("ticker_source", "Bloomberg")
            try:
                tickersource = TickerSource(tickersource_str)
            except (ValueError, KeyError):
                tickersource = TickerSource.BLOOMBERG

        # Extract time filters from metadata
        start_timestamp = context.metadata.get("start_time") if context.metadata else None
        end_timestamp = context.metadata.get("end_time") if context.metadata else None

        # If we have series_code, use ORM method for value data loading
        if series_code:
            try:
                return data_api.load_value_data_from_s3(
                    series_code=series_code,
                    tickersource=tickersource,
                )
            except Exception as exc:
                context.log.warning(
                    f"Failed to load value data for series_code '{series_code}': {exc}, "
                    "returning empty DataFrame"
                )
                return pd.DataFrame()

        context.log.warning(
            f"No series_code in metadata for asset {context.asset_key}, "
            "returning empty DataFrame"
        )
        return pd.DataFrame()

    def _convert_to_dataframe(self, obj: Any, context: OutputContext) -> pd.DataFrame:
        """Convert input object to pandas DataFrame.

        Args:
            obj: Input object (DataFrame, Series, or list)
            context: Output context for logging

        Returns:
            DataFrame
        """
        if isinstance(obj, pd.DataFrame):
            return obj
        elif isinstance(obj, pd.Series):
            return obj.to_frame()
        elif isinstance(obj, list):
            return pd.DataFrame(obj)
        else:
            raise ValueError(
                f"Cannot convert {type(obj)} to DataFrame for asset {context.asset_key}"
            )

    def _resolve_s3_path(self, context: OutputContext, df: pd.DataFrame) -> str:
        """Resolve S3 path for output based on context metadata or asset key.

        Uses build_s3_value_data_path from ORM layer for series_code-based paths.

        Args:
            context: Output context
            df: DataFrame being saved (unused, kept for compatibility)

        Returns:
            Relative S3 path
        """
        if context.metadata and "series_code" in context.metadata:
            series_code = context.metadata["series_code"]
            tickersource_str = context.metadata.get("ticker_source", "Bloomberg")
            tickersource = TickerSource(tickersource_str)
            return build_s3_value_data_path(series_code, tickersource)

        # Fallback to asset key-based path
        asset_key_str = "/".join(context.asset_key.path)
        return f"{self.s3_base_path}/{asset_key_str}/data.parquet"

    def _resolve_input_s3_path(self, context: InputContext) -> str:
        """Resolve S3 path for input based on context metadata or upstream asset key.

        Uses build_s3_value_data_path from ORM layer for series_code-based paths.

        Args:
            context: Input context

        Returns:
            Relative S3 path
        """
        if context.metadata and "series_code" in context.metadata:
            series_code = context.metadata["series_code"]
            tickersource_str = context.metadata.get("ticker_source", "Bloomberg")
            tickersource = TickerSource(tickersource_str)
            return build_s3_value_data_path(series_code, tickersource)

        # Use upstream asset key
        if context.upstream_output:
            upstream_key_str = "/".join(context.upstream_output.asset_key.path)
            return f"{self.s3_base_path}/{upstream_key_str}/data.parquet"

        # Fallback
        asset_key_str = "/".join(context.asset_key.path)
        return f"{self.s3_base_path}/{asset_key_str}/data.parquet"


@io_manager(required_resource_keys={"duckdb"})
def duckdb_io_manager(context) -> DuckDBIOManager:
    """Factory function for DuckDB IO Manager with S3 Parquet datalake.

    Uses DuckDBResource which already has S3 access configured via duckdb_datacacher.
    """
    return DuckDBIOManager(
        duckdb=context.resources.duckdb,
        s3_base_path=S3_BASE_PATH_VALUE_DATA,
    )
