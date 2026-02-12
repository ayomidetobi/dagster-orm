"""Parquet adapter for building parquet source queries.

This adapter abstracts parquet file access (local or S3).
It builds QueryBuilder-compatible FROM clauses without executing queries.
"""

from typing import Optional

from duckdb_tinyorm_py import QueryBuilder


class ParquetAdapter:
    """Adapter for building parquet source queries.

    Responsibilities:
    - Build read_parquet(...) sources
    - Abstract parquet file paths (local or S3)
    - Return QueryBuilder-compatible FROM clause

    Must:
    - Not execute queries
    - Not use connection
    - Not contain SQL execution logic
    """

    def build_parquet_source(self, uri: str) -> str:
        """Build parquet source expression for use in FROM clause.

        Args:
            uri: Full URI to parquet file (S3 or local path)

        Returns:
            SQL expression: read_parquet('uri')
        """
        return f"read_parquet('{uri}')"

    def build_query_builder_from_parquet(
        self, uri: str, table_alias: Optional[str] = None
    ) -> QueryBuilder:
        """Build QueryBuilder with parquet source.

        Creates a QueryBuilder that selects from a parquet file.
        The FROM clause will be replaced with the parquet source when building.

        Args:
            uri: Full URI to parquet file
            table_alias: Optional table alias for the parquet source

        Returns:
            QueryBuilder instance configured with parquet source
        """
        alias = table_alias or "_parquet_source"
        query_builder = QueryBuilder(alias)
        return query_builder

    def adapt_query_builder_for_parquet(
        self, query_builder: QueryBuilder, uri: str
    ) -> tuple[str, list]:
        """Adapt QueryBuilder output to use parquet source.

        Takes a QueryBuilder built with a placeholder table name and
        replaces the FROM clause with read_parquet(uri).

        Args:
            query_builder: QueryBuilder instance (built with placeholder table)
            uri: Full URI to parquet file

        Returns:
            Tuple of (adapted SQL, parameter list)
        """
        sql, params = query_builder.build()
        parquet_source = self.build_parquet_source(uri)
        adapted_sql = sql.replace(f"FROM {query_builder.table_name}", f"FROM {parquet_source}")
        return adapted_sql, params
