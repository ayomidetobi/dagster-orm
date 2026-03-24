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
        exclude: bool = False,
    ) -> pd.DataFrame:
        """Load metadata with optional filters applied at SQL level.

        Args:
            filters: Optional dictionary mapping column names to filter values
            control_type: Type of control table (default: 'metadata')
            exclude: If True, invert filter logic (exclude matching values)

        Returns:
            DataFrame with filtered metadata

        Raises:
            MetadataResolutionError: If loading fails or result is empty
        """
        try:
            uri = self._s3_adapter.get_metadata_uri(control_type)
            query_builder, param_values = self._build_filtered_query(filters, exclude)

            adapted_sql, builder_params = self._parquet_adapter.adapt_query_builder_for_parquet(
                query_builder, uri
            )

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
        self, filters: Optional[Dict[str, List[str]]], exclude: bool = False
    ) -> Tuple[QueryBuilder, List]:
        """Build QueryBuilder with WHERE clauses for filters.

        For IN clauses and single-value equality, predicates use ``?`` placeholders and values
        are appended to ``param_values`` in clause order. Mixing ``QueryBuilder.where()`` with
        manual IN clauses produced mis-ordered bind parameters vs SQL.

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
                            query_builder.where_clauses.append(f"{filter_field} != ?")
                            param_values.append(filter_values[0])
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
                        query_builder.where_clauses.append(f"{filter_field} = ?")
                        param_values.append(filter_values[0])
                    else:
                        placeholders = ", ".join(["?"] * len(filter_values))
                        query_builder.where_clauses.append(f"{filter_field} IN ({placeholders})")
                        param_values.extend(filter_values)

        return query_builder, param_values
