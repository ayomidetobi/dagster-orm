"""Metadata repository for loading and filtering metadata from parquet files."""

from typing import Dict, List, Optional, Tuple

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.schema import TableNames


class MetadataRepository:
    """Repository for loading metadata from parquet files.

    Responsibilities:
    - Load metadata from parquet (S3 or local)
    - Apply metadata filters
    - Return DataFrame

    Must:
    - Use QueryBuilder
    - Use DuckDbRepository for execution
    - Never access raw connection
    """

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        parquet_adapter: ParquetAdapter,
        s3_adapter: S3Adapter,
    ):
        """Initialize metadata repository.

        Args:
            duckdb_repository: DuckDbRepository for executing queries
            parquet_adapter: ParquetAdapter for building parquet sources
            s3_adapter: S3Adapter for URI resolution
        """
        self._repository = duckdb_repository
        self._parquet_adapter = parquet_adapter
        self._s3_adapter = s3_adapter

    def filter(
        self,
        filters: Optional[Dict[str, List[str]]] = None,
        control_type: str = TableNames.METADATA,
    ) -> pd.DataFrame:
        """Load metadata with optional filters applied at SQL level.

        Args:
            filters: Optional dictionary mapping column names to filter values
            control_type: Type of control table (default: 'metadata')

        Returns:
            DataFrame with filtered metadata

        Raises:
            MetadataResolutionError: If loading fails or result is empty
        """
        try:
            uri = self._s3_adapter.get_metadata_uri(control_type)
            query_builder, param_values = self._build_filtered_query(filters)

            adapted_sql, builder_params = self._parquet_adapter.adapt_query_builder_for_parquet(
                query_builder, uri
            )

            # Combine parameters: builder_params (from query_builder.where()) come first,
            # then param_values (from manually added IN clauses)
            all_params = builder_params + param_values

            if all_params:
                result = self._repository.execute_raw_sql(adapted_sql, all_params)
            else:
                result = self._repository.execute_raw_sql(adapted_sql)

            if result.empty:
                raise MetadataResolutionError(
                    f"Metadata table '{control_type}' is empty or does not exist"
                )

            return result

        except MetadataResolutionError:
            raise
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load metadata: {exc}") from exc

    def _build_filtered_query(
        self, filters: Optional[Dict[str, List[str]]]
    ) -> Tuple[QueryBuilder, List]:
        """Build QueryBuilder with WHERE clauses for filters.

        Note: QueryBuilder.where() already handles parameters internally.
        For IN clauses, we manually add placeholders and track parameters separately
        because QueryBuilder.where_in() uses named parameters but DuckDB needs positional.

        Args:
            filters: Optional dictionary mapping column names to filter values

        Returns:
            Tuple of (QueryBuilder instance, list of parameter values)
            Note: param_values only contains values for manually added IN clauses.
            Values from query_builder.where() are already in query_builder.params.
        """
        query_builder = QueryBuilder("_parquet_source")
        param_values: list = []

        if filters:
            for filter_field, filter_values in filters.items():
                if filter_values:
                    if len(filter_values) == 1:
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
