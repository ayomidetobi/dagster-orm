"""Validation repository for validating metadata against wide-format lookup tables."""

from typing import Dict, List, Optional, Tuple

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.schema import (
    LOOKUP_TABLE_PROCESSING_ORDER,
    MetadataColumns,
    TableNames,
)


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
        self, filters: Optional[Dict[str, List[str]]]
    ) -> Tuple[QueryBuilder, List]:
        """Build QueryBuilder with WHERE clauses for filters.

        Note: QueryBuilder.where_in() uses named parameters, but DuckDB needs
        positional parameters. We handle IN clauses manually with ? placeholders.

        Args:
            filters: Optional dictionary mapping column names to filter values

        Returns:
            Tuple of (QueryBuilder instance, list of parameter values)
        """
        query_builder = QueryBuilder("_parquet_source")
        param_values: list = []

        if filters:
            for filter_field, filter_values in filters.items():
                if filter_values:
                    if len(filter_values) == 1:
                        query_builder.where(filter_field, "=", filter_values[0])
                        param_values.append(filter_values[0])
                    else:
                        placeholders = ", ".join(["?"] * len(filter_values))
                        query_builder.where_clauses.append(f"{filter_field} IN ({placeholders})")
                        param_values.extend(filter_values)

        return query_builder, param_values

    def _build_exists_clauses_wide_lookup(self, lookup_table_name: str) -> str:
        """Build EXISTS clauses for wide-format lookup parquet.

        Each column in LOOKUP_TABLE_PROCESSING_ORDER is semi-joined individually.

        Args:
            lookup_table_name: Name of the temp lookup table

        Returns:
            SQL string with AND-ed EXISTS clauses
        """
        clauses = []
        for lookup_col in LOOKUP_TABLE_PROCESSING_ORDER:
            clause = f"""
                EXISTS (
                    SELECT 1
                    FROM {lookup_table_name} AS l
                    WHERE l.{lookup_col} = m.{lookup_col}
                      AND l.{lookup_col} IS NOT NULL
                )
            """
            clauses.append(clause)
        return " AND ".join(clauses)

    def filter_with_validation(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        control_type: str = TableNames.METADATA,
    ) -> pd.DataFrame:
        """Return metadata rows fully validated against wide-format lookup parquet.

        Args:
            filters: Optional dictionary mapping column names to filter values
            control_type: Type of control table (default: 'metadata')

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
            query_builder, param_values = self._build_filtered_query(filters)

            # Adapt the base query to use parquet source
            adapted_sql, builder_params = self._parquet_adapter.adapt_query_builder_for_parquet(
                query_builder, metadata_uri
            )

            # Replace the SELECT clause to use alias 'm' and add EXISTS clauses
            parquet_source = self._parquet_adapter.build_parquet_source(metadata_uri)
            exists_clauses = self._build_exists_clauses_wide_lookup(lookup_table_name)

            # Build final SQL with EXISTS clauses
            # Replace FROM clause to add alias, and append EXISTS clauses to WHERE
            final_sql = adapted_sql.replace(
                f"FROM {parquet_source}",
                f"FROM {parquet_source} AS m"
            )

            # Add EXISTS clauses to WHERE clause
            if "WHERE" in final_sql:
                final_sql = f"{final_sql} AND {exists_clauses}"
            else:
                final_sql = f"{final_sql} WHERE {exists_clauses}"

            all_params = param_values + builder_params

            if all_params:
                result = self._repository.execute_raw_sql(final_sql, all_params)
            else:
                result = self._repository.execute_raw_sql(final_sql)

            if result.empty:
                raise MetadataResolutionError(
                    f"No metadata rows found after validation for control_type '{control_type}'"
                )

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

            # Build base WHERE clause and parameters from filters
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

            # Build UNION ALL query to return one row per invalid column
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
                return pd.DataFrame(columns=[
                    MetadataColumns.SERIES_CODE,
                    MetadataColumns.SERIES_NAME,
                    "invalid_column",
                    "invalid_value"
                ])

            # Each UNION part needs the same parameters, so we repeat them
            # Since all parts have identical WHERE clauses, we can reuse params
            sql_query = " UNION ALL ".join(union_parts)

            if param_values:
                result = self._repository.execute_raw_sql(sql_query, param_values)
            else:
                result = self._repository.execute_raw_sql(sql_query)

            return result

        finally:
            self._temp_table_manager.drop_temp_table(lookup_table_name)
