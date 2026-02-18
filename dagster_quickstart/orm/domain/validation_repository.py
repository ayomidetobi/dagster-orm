"""Validation repository for validating metadata against wide-format lookup tables."""

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.schema import (
    LOOKUP_TABLE_PROCESSING_ORDER,
    MetadataColumns,
    TableNames,
    ValueColumns,
)
from dagster_quickstart.utils.datetime_utils import normalize_date_to_utc


class ValidationRepository:
    """Repository for validating metadata against wide-format lookup tables using DuckDB temp tables."""

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        parquet_adapter: ParquetAdapter,
        s3_adapter: S3Adapter,
        temp_table_manager: TempTableManager,
    ):
        """Initialize validation repository.

        Args:
            duckdb_repository: DuckDbRepository for executing queries
            parquet_adapter: ParquetAdapter for building parquet sources
            s3_adapter: S3Adapter for URI resolution
            temp_table_manager: TempTableManager for managing temp tables
        """
        self._repository = duckdb_repository
        self._parquet_adapter = parquet_adapter
        self._s3_adapter = s3_adapter
        self._temp_table_manager = temp_table_manager

    def _build_filtered_query(
        self, filters: Optional[Dict[str, List[str]]], exclude: bool = False
    ) -> Tuple[QueryBuilder, List]:
        """Build QueryBuilder with WHERE clauses for filters.

        Note: QueryBuilder.where_in() uses named parameters, but DuckDB needs
        positional parameters. We handle IN clauses manually with ? placeholders.

        Args:
            filters: Optional dictionary mapping column names to filter values
            exclude: If True, invert filter logic (exclude matching values)

        Returns:
            Tuple of (QueryBuilder instance, list of parameter values)
        """
        query_builder = QueryBuilder("_parquet_source")
        param_values: list = []

        if filters:
            for filter_field, filter_values in filters.items():
                if exclude:
                    # Exclude mode: ignore empty lists, use NOT IN for non-empty lists
                    if filter_values:
                        if len(filter_values) == 1:
                            query_builder.where(filter_field, "!=", filter_values[0])
                        else:
                            placeholders = ", ".join(["?"] * len(filter_values))
                            query_builder.where_clauses.append(
                                f"{filter_field} NOT IN ({placeholders})"
                            )
                            param_values.extend(filter_values)
                else:
                    # Include mode: empty list means WHERE 1=0 (no matches)
                    if not filter_values:
                        query_builder.where_clauses.append("1=0")
                    elif len(filter_values) == 1:
                        # Use QueryBuilder.where() which handles parameters internally
                        query_builder.where(filter_field, "=", filter_values[0])
                        # Don't add to param_values - query_builder.build() will return it
                    else:
                        # For IN clauses, manually add placeholders and track parameters
                        # because QueryBuilder.where_in() uses named params but DuckDB needs positional
                        placeholders = ", ".join(["?"] * len(filter_values))
                        query_builder.where_clauses.append(f"{filter_field} IN ({placeholders})")
                        param_values.extend(filter_values)

        return query_builder, param_values

    def _build_exists_clauses_wide_lookup(self, lookup_table_name: str) -> str:
        """Build EXISTS clauses for wide-format lookup parquet.

        Each column in LOOKUP_TABLE_PROCESSING_ORDER is semi-joined individually.
        Only validates columns that have non-NULL values in metadata.
        NULL values in metadata are skipped (not validated).

        Args:
            lookup_table_name: Name of the temp lookup table

        Returns:
            SQL string with AND-ed EXISTS clauses
        """
        clauses = []
        for lookup_col in LOOKUP_TABLE_PROCESSING_ORDER:
            # Only validate if metadata column has a value (is NOT NULL)
            # If metadata column is NULL, skip validation for that column
            clause = f"""
                (m.{lookup_col} IS NULL OR EXISTS (
                    SELECT 1
                    FROM {lookup_table_name} AS l
                    WHERE l.{lookup_col} = m.{lookup_col}
                      AND l.{lookup_col} IS NOT NULL
                ))
            """
            clauses.append(clause)
        return " AND ".join(clauses)

    def filter_with_validation(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        control_type: str = TableNames.METADATA,
        exclude: bool = False,
    ) -> pd.DataFrame:
        """Return metadata rows fully validated against wide-format lookup parquet.

        Args:
            filters: Optional dictionary mapping column names to filter values
            control_type: Type of control table (default: 'metadata')
            exclude: If True, invert filter logic (exclude matching values)

        Returns:
            DataFrame with validated metadata rows

        Raises:
            MetadataResolutionError: If no rows found after validation
        """
        lookup_table_name = "_temp_lookup_validation"
        try:
            lookup_uri = self._s3_adapter.get_lookup_uri()
            self._temp_table_manager.create_temp_table_from_parquet(
                lookup_uri, lookup_table_name, force_recreate=True
            )

            metadata_uri = self._s3_adapter.get_metadata_uri(control_type)
            query_builder, param_values = self._build_filtered_query(filters, exclude)

            # Adapt the base query to use parquet source
            adapted_sql, builder_params = self._parquet_adapter.adapt_query_builder_for_parquet(
                query_builder, metadata_uri
            )

            # Replace the SELECT clause to use alias 'm' and add EXISTS clauses
            parquet_source = self._parquet_adapter.build_parquet_source(metadata_uri)
            exists_clauses = self._build_exists_clauses_wide_lookup(lookup_table_name)

            # Build final SQL with EXISTS clauses
            # Replace FROM clause to add alias, and append EXISTS clauses to WHERE
            final_sql = adapted_sql.replace(f"FROM {parquet_source}", f"FROM {parquet_source} AS m")

            # Add EXISTS clauses to WHERE clause
            if "WHERE" in final_sql:
                final_sql = f"{final_sql} AND {exists_clauses}"
            else:
                final_sql = f"{final_sql} WHERE {exists_clauses}"

            # Combine parameters: builder_params (from query_builder.where()) come first,
            # then param_values (from manually added IN clauses)
            all_params = builder_params + param_values

            if all_params:
                result = self._repository.execute_raw_sql(final_sql, all_params)
            else:
                result = self._repository.execute_raw_sql(final_sql)

            # Return empty DataFrame instead of raising error
            # This allows callers to handle empty results gracefully
            # (e.g., when filters are too restrictive or lookup table is incomplete)
            return result

        finally:
            self._temp_table_manager.drop_temp_table(lookup_table_name)

    def get_invalid_rows(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        control_type: str = TableNames.METADATA,
    ) -> pd.DataFrame:
        """Return metadata rows with invalid lookup values, including column name and invalid value.

        Returns one row per invalid column per metadata row with:
        - series_code
        - series_name
        - invalid_column (name of the column with invalid value)
        - invalid_value (the invalid value)

        Args:
            filters: Optional dictionary mapping column names to filter values
            control_type: Type of control table (default: 'metadata')

        Returns:
            DataFrame with invalid rows (one per invalid column per metadata row)
        """
        lookup_table_name = "_temp_lookup_validation"
        try:
            lookup_uri = self._s3_adapter.get_lookup_uri()
            self._temp_table_manager.create_temp_table_from_parquet(
                lookup_uri, lookup_table_name, force_recreate=True
            )

            metadata_uri = self._s3_adapter.get_metadata_uri(control_type)
            parquet_source = self._parquet_adapter.build_parquet_source(metadata_uri)

            where_clause_parts = []
            param_values: list = []

            if filters:
                for filter_field, filter_values in filters.items():
                    if filter_values:
                        if len(filter_values) == 1:
                            where_clause_parts.append(f"m.{filter_field} = ?")
                            param_values.append(filter_values[0])
                        else:
                            placeholders = ", ".join(["?"] * len(filter_values))
                            where_clause_parts.append(f"m.{filter_field} IN ({placeholders})")
                            param_values.extend(filter_values)

            base_where = " AND ".join(where_clause_parts) if where_clause_parts else "1=1"

            union_parts = []
            for lookup_col in LOOKUP_TABLE_PROCESSING_ORDER:
                union_part = f"""
                    SELECT 
                        m.{MetadataColumns.SERIES_CODE},
                        m.{MetadataColumns.SERIES_NAME},
                        '{lookup_col}' AS invalid_column,
                        CAST(m.{lookup_col} AS VARCHAR) AS invalid_value
                    FROM {parquet_source} AS m
                    WHERE {base_where}
                      AND m.{lookup_col} IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {lookup_table_name} AS l
                          WHERE l.{lookup_col} = m.{lookup_col}
                          AND l.{lookup_col} IS NOT NULL
                      )
                """
                union_parts.append(union_part.strip())

            if not union_parts:
                return pd.DataFrame(
                    columns=[
                        MetadataColumns.SERIES_CODE,
                        MetadataColumns.SERIES_NAME,
                        "invalid_column",
                        "invalid_value",
                    ]
                )

            sql_query = " UNION ALL ".join(union_parts)

            if param_values:
                result = self._repository.execute_raw_sql(sql_query, param_values)
            else:
                result = self._repository.execute_raw_sql(sql_query)

            return result

        finally:
            self._temp_table_manager.drop_temp_table(lookup_table_name)

    def validate_date_range_for_force_refresh(
        self, force_refresh: bool, start_date: Any, end_date: Any
    ) -> None:
        """Validate date range parameters when force_refresh is enabled.

        Args:
            force_refresh: Whether force refresh is enabled
            start_date: Start date (datetime or date string)
            end_date: End date (datetime or date string)

        Raises:
            ValueError: If force_refresh=True but start_date or end_date is missing,
                or if start_date > end_date
        """
        if not force_refresh:
            return

        if start_date is None or end_date is None:
            raise ValueError(
                "force_refresh=True requires both start_date and end_date to be provided"
            )

        start_date_utc = normalize_date_to_utc(start_date)
        end_date_utc = normalize_date_to_utc(end_date)
        if start_date_utc > end_date_utc:
            raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

    def validate_value_dataframe_columns(
        self, df: pd.DataFrame, df_name: str = "dataframe"
    ) -> None:
        """Validate that DataFrame has required value data columns.

        Args:
            df: DataFrame to validate
            df_name: Name of dataframe for error messages

        Raises:
            ValueError: If required columns are missing
        """
        required_cols = [
            ValueColumns.SERIES_CODE,
            ValueColumns.TIMESTAMP,
            ValueColumns.VALUE,
        ]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"{df_name} missing required columns: {missing}")

    def validate_data_points_structure(self, points: list, required_columns: list[str]) -> bool:
        """Validate that data points have required structure.

        Args:
            points: List of data point dicts
            required_columns: List of required column names

        Returns:
            True if valid, False otherwise
        """
        if not points:
            return False

        if not isinstance(points[0], dict):
            return False

        missing_columns = [col for col in required_columns if col not in points[0]]
        return len(missing_columns) == 0
