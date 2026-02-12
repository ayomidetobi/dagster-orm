"""DuckDB repository for executing queries.

This is the ONLY class allowed to execute SQL.
All query execution must go through this class.
"""

from typing import Any, List, Optional

import duckdb
import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.exceptions import InvalidQueryError


class DuckDbRepository:
    """Repository for executing DuckDB queries.

    This class wraps a DuckDB connection and provides methods for executing
    queries built with QueryBuilder. It does NOT create or own the connection.

    Rules:
    - Must NOT create connection
    - Must NOT modify connection config
    - Must NOT assume ownership of connection
    - Only wraps execution
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """Initialize repository with injected connection.

        Args:
            connection: DuckDB connection object (must be provided externally)

        Raises:
            ValueError: If connection is None
        """
        if connection is None:
            raise ValueError("Connection cannot be None")
        self._connection = connection

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the underlying DuckDB connection."""
        return self._connection

    def execute_builder(self, query_builder: QueryBuilder) -> duckdb.DuckDBPyRelation:
        """Execute a QueryBuilder and return DuckDB relation.

        Args:
            query_builder: QueryBuilder instance with query configured

        Returns:
            DuckDB relation object

        Raises:
            InvalidQueryError: If query execution fails
        """
        try:
            sql, params = query_builder.build()
            if params:
                return self._connection.execute(sql, params)
            return self._connection.execute(sql)
        except Exception as exc:
            raise InvalidQueryError(f"Error executing query: {str(exc)}") from exc

    def fetch_df(self, query_builder: QueryBuilder) -> pd.DataFrame:
        """Execute a QueryBuilder and return DataFrame.

        Args:
            query_builder: QueryBuilder instance with query configured

        Returns:
            pandas DataFrame with query results

        Raises:
            InvalidQueryError: If query execution fails
        """
        try:
            sql, params = query_builder.build()
            if params:
                return self._connection.execute(sql, params).df()
            return self._connection.execute(sql).df()
        except Exception as exc:
            raise InvalidQueryError(f"Error executing query: {str(exc)}") from exc

    def execute_raw_sql(self, sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        """Execute raw SQL query (for DDL operations like COPY).

        This method should be used sparingly, only for operations that
        cannot be expressed through QueryBuilder (e.g., COPY statements).

        Args:
            sql: Raw SQL query string
            params: Optional list of positional parameters

        Returns:
            pandas DataFrame with query results

        Raises:
            InvalidQueryError: If query execution fails
        """
        try:
            if params:
                return self._connection.execute(sql, params).df()
            return self._connection.execute(sql).df()
        except Exception as exc:
            raise InvalidQueryError(f"Error executing raw SQL: {str(exc)}") from exc

    def execute_raw_relation(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute raw SQL query and return relation (for DDL operations).

        Args:
            sql: Raw SQL query string
            params: Optional list of positional parameters

        Returns:
            DuckDB relation object

        Raises:
            InvalidQueryError: If query execution fails
        """
        try:
            if params:
                return self._connection.execute(sql, params)
            return self._connection.execute(sql)
        except Exception as exc:
            raise InvalidQueryError(f"Error executing raw SQL: {str(exc)}") from exc

    def copy_builder_to_parquet(self, query_builder: QueryBuilder, destination_uri: str) -> None:
        """Copy query results to Parquet file.

        Uses QueryBuilder to build SELECT query, then wraps it in COPY statement.

        Args:
            query_builder: QueryBuilder instance with SELECT query configured
            destination_uri: Full URI to destination Parquet file (S3 or local)

        Raises:
            InvalidQueryError: If copy operation fails
        """
        try:
            select_sql, params = query_builder.build()
            copy_sql = f"COPY ({select_sql}) TO '{destination_uri}' (FORMAT PARQUET)"
            if params:
                self._connection.execute(copy_sql, params)
            else:
                self._connection.execute(copy_sql)
        except Exception as exc:
            raise InvalidQueryError(f"Error copying to Parquet: {str(exc)}") from exc

    def register_dataframe(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """Register a pandas DataFrame as a temporary table.

        Args:
            dataframe: DataFrame to register
            table_name: Name for the temporary table

        Raises:
            InvalidQueryError: If registration fails
        """
        try:
            self._connection.register(table_name, dataframe)
        except Exception as exc:
            raise InvalidQueryError(f"Error registering DataFrame: {str(exc)}") from exc

    def unregister_table(self, table_name: str) -> None:
        """Unregister a temporary table.

        Args:
            table_name: Name of the table to unregister

        Raises:
            InvalidQueryError: If unregistration fails
        """
        try:
            self._connection.unregister(table_name)
        except Exception as exc:
            raise InvalidQueryError(f"Error unregistering table: {str(exc)}") from exc
