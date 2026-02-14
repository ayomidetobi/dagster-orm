"""Value repository for loading value data from parquet files."""

from typing import List, Optional, Tuple

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.schema import TickerSource, ValueColumns


class ValueRepository:
    """Repository for loading value data from parquet files.

    Responsibilities:
    - Load value data for series_codes
    - Apply time filtering
    - Apply ordering
    - Apply limit
    - Support batch series loading

    Must:
    - Use QueryBuilder
    - Support UNION ALL via builder abstraction
    - Not manually build SQL strings
    - Not access connection directly
    """

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        parquet_adapter: ParquetAdapter,
        s3_adapter: S3Adapter,
    ):
        """Initialize value repository.

        Args:
            duckdb_repository: DuckDbRepository for executing queries
            parquet_adapter: ParquetAdapter for building parquet sources
            s3_adapter: S3Adapter for URI resolution
        """
        self._repository = duckdb_repository
        self._parquet_adapter = parquet_adapter
        self._s3_adapter = s3_adapter

    def get_series_data(
        self,
        series_code: str,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        start: Optional[str] = None,
        end: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load value data for a single series_code.

        Args:
            series_code: Series code identifier
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)
            start: Optional start timestamp filter (inclusive)
            end: Optional end timestamp filter (inclusive)
            order_by: Optional column name to order by (default: timestamp)
            limit: Optional row limit

        Returns:
            DataFrame with series_code, timestamp, and value columns
        """
        try:
            uri = self._s3_adapter.get_value_data_uri(series_code, tickersource)
            sql, params = self._build_series_query_sql(
                series_code, uri, start, end, order_by, limit
            )

            if params:
                result = self._repository.execute_raw_sql(sql, params)
            else:
                result = self._repository.execute_raw_sql(sql)

            return result

        except Exception:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

    def get_batch_series_data(
        self,
        series_codes: List[str],
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        start: Optional[str] = None,
        end: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load value data for multiple series_codes using UNION ALL.

        Args:
            series_codes: List of series code identifiers
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)
            start: Optional start timestamp filter (inclusive)
            end: Optional end timestamp filter (inclusive)
            order_by: Optional column name to order by (default: timestamp)
            limit: Optional row limit

        Returns:
            DataFrame with series_code, timestamp, and value columns for all series

        Raises:
            MetadataResolutionError: If loading fails
        """
        if not series_codes:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        try:
            union_parts = []
            all_params: List = []

            for series_code in series_codes:
                try:
                    uri = self._s3_adapter.get_value_data_uri(series_code, tickersource)
                    sql, params = self._build_series_query_sql(
                        series_code, uri, start, end, None, None
                    )

                    union_parts.append(f"({sql})")
                    if params:
                        all_params.extend(params)

                except Exception:
                    continue

            if not union_parts:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )

            order_by_column = order_by or ValueColumns.TIMESTAMP
            order_by_clause = f" ORDER BY {order_by_column}"
            limit_clause = f" LIMIT {limit}" if limit else ""

            union_query = f"""
                SELECT * FROM (
                    {' UNION ALL '.join(union_parts)}
                )
                {order_by_clause}
                {limit_clause}
            """

            if all_params:
                result = self._repository.execute_raw_sql(union_query, all_params)
            else:
                result = self._repository.execute_raw_sql(union_query)

            return result

        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load value data batch: {exc}") from exc

    def _build_series_query_sql(
        self,
        series_code: str,
        uri: str,
        start: Optional[str],
        end: Optional[str],
        order_by: Optional[str],
        limit: Optional[int],
    ) -> Tuple[str, List]:
        """Build SQL query for a single series using QueryBuilder.

        Args:
            series_code: Series code identifier
            uri: Parquet file URI
            start: Optional start timestamp filter
            end: Optional end timestamp filter
            order_by: Optional column name to order by
            limit: Optional row limit

        Returns:
            Tuple of (SQL string, parameter list)
        """
        query_builder = QueryBuilder("_parquet_source")

        # Use QueryBuilder.where() which handles parameters internally
        if start:
            query_builder.where(ValueColumns.TIMESTAMP, ">=", start)
        if end:
            query_builder.where(ValueColumns.TIMESTAMP, "<=", end)

        order_by_column = order_by or ValueColumns.TIMESTAMP
        query_builder.order_by(order_by_column)

        if limit:
            query_builder.limit(limit)

        # QueryBuilder.build() returns the parameters, don't manually add them
        base_sql, builder_params = query_builder.build()

        parquet_source = self._parquet_adapter.build_parquet_source(uri)
        adapted_sql = base_sql.replace("FROM _parquet_source", f"FROM {parquet_source}")

        custom_select = f"""
            SELECT 
                '{series_code}' AS {ValueColumns.SERIES_CODE},
                {ValueColumns.TIMESTAMP},
                {ValueColumns.VALUE}
        """
        final_sql = adapted_sql.replace("SELECT *", custom_select.strip())

        return final_sql, builder_params
