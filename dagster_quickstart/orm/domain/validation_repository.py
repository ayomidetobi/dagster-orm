"""Validation repository for validating metadata against wide-format lookup tables."""

from typing import Dict, List, Optional, Tuple
import pandas as pd

from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, LOOKUP_TABLE_PROCESSING_ORDER


class ValidationRepository:
    """Repository for validating metadata against wide-format lookup tables using DuckDB temp tables."""

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        s3_adapter: S3Adapter,
        temp_table_manager: TempTableManager,
    ):
        self._repository = duckdb_repository
        self._s3_adapter = s3_adapter
        self._temp_table_manager = temp_table_manager

    def _build_filter_where_clause(
        self, filters: Optional[Dict[str, List[str]]], table_alias: str = "m"
    ) -> Tuple[str, List]:
        """Build WHERE clause string from filters."""
        where_clause = ""
        param_values: List = []

        if filters:
            for field, values in filters.items():
                if values:
                    if len(values) == 1:
                        where_clause += f" AND {table_alias}.{field} = ?"
                        param_values.append(values[0])
                    else:
                        placeholders = ", ".join(["?"] * len(values))
                        where_clause += f" AND {table_alias}.{field} IN ({placeholders})"
                        param_values.extend(values)

        return where_clause, param_values

    def _build_exists_clauses_wide_lookup(self, lookup_table_name: str) -> str:
        """
        Build EXISTS clauses for wide-format lookup parquet.

        Each column in LOOKUP_TABLE_PROCESSING_ORDER is semi-joined individually.
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
        """Return metadata rows fully validated against wide-format lookup parquet."""
        lookup_table_name = "_temp_lookup_validation"
        try:
            lookup_uri = self._s3_adapter.get_lookup_uri()
            self._temp_table_manager.create_temp_table_from_parquet(
                lookup_uri, lookup_table_name, force_recreate=True
            )

            metadata_uri = self._s3_adapter.get_metadata_uri(control_type)
            where_clause, param_values = self._build_filter_where_clause(filters)
            exists_clauses = self._build_exists_clauses_wide_lookup(lookup_table_name)

            sql_query = f"""
                SELECT m.*
                FROM read_parquet('{metadata_uri}') AS m
                WHERE 1=1 {where_clause}
                  AND {exists_clauses}
            """

            if param_values:
                result = self._repository.execute_raw_sql(sql_query, param_values)
            else:
                result = self._repository.execute_raw_sql(sql_query)

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
        """
        lookup_table_name = "_temp_lookup_validation"
        try:
            lookup_uri = self._s3_adapter.get_lookup_uri()
            self._temp_table_manager.create_temp_table_from_parquet(
                lookup_uri, lookup_table_name, force_recreate=True
            )

            metadata_uri = self._s3_adapter.get_metadata_uri(control_type)
            where_clause, param_values = self._build_filter_where_clause(filters)

            # Build UNION ALL query to return one row per invalid column
            union_parts = []
            for lookup_col in LOOKUP_TABLE_PROCESSING_ORDER:
                union_part = f"""
                    SELECT 
                        m.{MetadataColumns.SERIES_CODE},
                        m.{MetadataColumns.SERIES_NAME},
                        '{lookup_col}' AS invalid_column,
                        CAST(m.{lookup_col} AS VARCHAR) AS invalid_value
                    FROM read_parquet('{metadata_uri}') AS m
                    WHERE 1=1 {where_clause}
                      AND m.{lookup_col} IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {lookup_table_name} AS l
                          WHERE l.{lookup_col} = m.{lookup_col}
                          AND l.{lookup_col} IS NOT NULL
                      )
                """
                union_parts.append(union_part)

            if not union_parts:
                return pd.DataFrame(columns=[
                    MetadataColumns.SERIES_CODE,
                    MetadataColumns.SERIES_NAME,
                    "invalid_column",
                    "invalid_value"
                ])

            sql_query = " UNION ALL ".join(union_parts)

            if param_values:
                result = self._repository.execute_raw_sql(sql_query, param_values)
            else:
                result = self._repository.execute_raw_sql(sql_query)

            return result

        finally:
            self._temp_table_manager.drop_temp_table(lookup_table_name)
