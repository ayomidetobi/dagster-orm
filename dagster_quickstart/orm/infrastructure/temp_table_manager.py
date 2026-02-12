"""Temp table manager for lifecycle management of temporary tables.

Manages registration, tracking, and cleanup of temporary tables.
"""

from typing import Dict, Optional
import uuid

import pandas as pd

from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository


class TempTableManager:
    """Manager for temporary table lifecycle.

    Responsibilities:
    - Register pandas DataFrame as temp table
    - Drop temp table
    - Track lifecycle

    Must:
    - Not create connection
    - Not contain query building logic
    - Use DuckDbRepository for all operations
    """

    def __init__(self, duckdb_repository: DuckDbRepository):
        """Initialize temp table manager with DuckDB repository.

        Args:
            duckdb_repository: DuckDbRepository instance for executing operations
        """
        self._repository = duckdb_repository
        self._registry: Dict[str, bool] = {}

    def temp_table_exists(self, table_name: str) -> bool:
        """Check if a temporary table exists in the registry.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists in registry, False otherwise
        """
        return table_name in self._registry

    def get_or_create_temp_table(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        force_recreate: bool = False,
    ) -> str:
        """Get existing temp table or create a new one if it doesn't exist.

        Prevents creating duplicate tables by checking the registry first.

        Args:
            dataframe: DataFrame to convert to temp table (only used if creating new)
            table_name: Name of the table (required for reuse)
            force_recreate: If True, drop and recreate even if table exists

        Returns:
            Name of the temp table (same as input table_name)

        Raises:
            MetadataResolutionError: If table creation fails
        """
        if table_name in self._registry and not force_recreate:
            return table_name

        if force_recreate and table_name in self._registry:
            self.drop_temp_table(table_name)

        try:
            self._repository.register_dataframe(dataframe, table_name)
            self._registry[table_name] = True
            return table_name
        except Exception as exc:
            raise MetadataResolutionError(
                f"Failed to create temp table '{table_name}': {exc}"
            ) from exc

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
        if table_name is None:
            table_name = f"_temp_orm_{uuid.uuid4().hex[:8]}"

        return self.get_or_create_temp_table(dataframe, table_name, force_recreate=True)

    def drop_temp_table(self, table_name: str) -> None:
        """Drop temporary table and remove from registry.

        Args:
            table_name: Name of temporary table to drop
        """
        try:
            self._repository.unregister_table(table_name)
        except Exception:
            pass
        finally:
            self._registry.pop(table_name, None)

    def get_temp_table_name(self, table_name: str) -> Optional[str]:
        """Get temp table name if it exists in registry.

        Args:
            table_name: Name of the table to get

        Returns:
            Table name if exists, None otherwise
        """
        if table_name in self._registry:
            return table_name
        return None

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

        Raises:
            MetadataResolutionError: If table creation fails
        """
        if table_name in self._registry and not force_recreate:
            return table_name

        if force_recreate and table_name in self._registry:
            self.drop_temp_table(table_name)

        try:
            create_temp_sql = f"""
                CREATE TEMP TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}')
            """
            self._repository.execute_raw_relation(create_temp_sql)
            self._registry[table_name] = True
            return table_name
        except Exception as exc:
            raise MetadataResolutionError(
                f"Failed to create temp table '{table_name}' from CSV '{csv_path}': {exc}"
            ) from exc

    def create_temp_table_from_sql(
        self, sql_query: str, table_name: str, force_recreate: bool = False
    ) -> str:
        """Create temporary table from SQL query and register it.

        Args:
            sql_query: SQL query that creates the temp table
            table_name: Name of the temp table to create
            force_recreate: If True, drop and recreate even if table exists

        Returns:
            Name of the created temp table

        Raises:
            MetadataResolutionError: If table creation fails
        """
        if table_name in self._registry and not force_recreate:
            return table_name

        if force_recreate and table_name in self._registry:
            self.drop_temp_table(table_name)

        try:
            self._repository.execute_raw_relation(sql_query)
            self._registry[table_name] = True
            return table_name
        except Exception as exc:
            raise MetadataResolutionError(
                f"Failed to create temp table '{table_name}' from SQL: {exc}"
            ) from exc

    def create_temp_table_from_parquet(
        self, parquet_uri: str, table_name: str, force_recreate: bool = False
    ) -> str:
        """Create temporary table from Parquet file and register it.

        Args:
            parquet_uri: URI to the Parquet file (S3 or local path)
            table_name: Name of the temp table to create
            force_recreate: If True, drop and recreate even if table exists

        Returns:
            Name of the created temp table

        Raises:
            MetadataResolutionError: If table creation fails
        """
        if table_name in self._registry and not force_recreate:
            return table_name

        if force_recreate and table_name in self._registry:
            self.drop_temp_table(table_name)

        try:
            create_temp_sql = f"""
                CREATE TEMP TABLE {table_name} AS
                SELECT * FROM read_parquet('{parquet_uri}')
            """
            self._repository.execute_raw_relation(create_temp_sql)
            self._registry[table_name] = True
            return table_name
        except Exception as exc:
            raise MetadataResolutionError(
                f"Failed to create temp table '{table_name}' from Parquet '{parquet_uri}': {exc}"
            ) from exc
