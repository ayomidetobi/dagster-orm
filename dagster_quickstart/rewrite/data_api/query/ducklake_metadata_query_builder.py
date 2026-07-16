"""SQL builder for DuckLake metadata storage."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from resources.duckdb_datacacher import SQL
from rewrite.data_api.query.snapshot import table_reference


class DuckLakeMetadataQueryBuilder:
    """
    Builds SQL statements for metadata storage.

    This class is responsible only for SQL generation.
    """

    def __init__(
        self,
        *,
        table_name: str,
    ) -> None:
        self._table = table_name

    def build_get_metadata(
        self,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> SQL:
        """
        Build SQL for metadata retrieval.
        """

        sql = SQL(
            """
            SELECT *
            FROM $table
            """,
            table=table_reference(self._table, version=version, as_of=as_of),
        )

        if filters:
            sql += self._where_clause(
                filters,
                exclude=exclude,
            )

        return sql

    def build_ensure_table(self, relation: str) -> SQL:
        """
        Create the table from a sample relation's schema if it doesn't exist yet.

        Metadata columns are meant to stay flexible (arbitrary per-asset-class
        attributes), so the schema is inferred from the first write rather
        than declared up front.
        """

        return SQL(
            "CREATE TABLE IF NOT EXISTS $table AS SELECT * FROM $relation LIMIT 0",
            table=SQL.identifier(self._table),
            relation=SQL.identifier(relation),
        )

    def build_save_metadata(
        self,
        relation: str,
    ) -> SQL:
        """
        Build an append-only INSERT statement.

        DuckLake snapshots every write, so history is preserved without a
        manual upsert/merge strategy. Call build_ensure_table() first so the
        table exists.
        """

        return SQL(
            "INSERT INTO $table SELECT * FROM $relation",
            table=SQL.identifier(self._table),
            relation=SQL.identifier(relation),
        )

    def build_columns(self) -> SQL:
        """Build a schema-only query (no rows) to discover available column names."""

        return SQL(
            "SELECT * FROM $table LIMIT 0",
            table=SQL.identifier(self._table),
        )

    def build_distinct_values(
        self,
        column: str,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
    ) -> SQL:
        """Build a query for the distinct, non-null values of a single column.

        Optionally narrowed by filters, so contextual options (e.g. currency
        values within asset_class=Equity) are computed by the database
        instead of pulling the whole table into pandas.
        """

        clauses = [
            SQL("$is_not_null_column IS NOT NULL", is_not_null_column=SQL.identifier(column))
        ]
        if filters:
            clauses.extend(self._filter_clauses(filters, exclude=exclude))

        where = SQL.join(clauses, SQL(" AND "), prefix=" WHERE ")

        sql = SQL(
            "SELECT DISTINCT $column AS value FROM $table",
            column=SQL.identifier(column),
            table=SQL.identifier(self._table),
        )
        return sql + where

    def _filter_clauses(
        self,
        filters: Mapping[str, Sequence[str]],
        *,
        exclude: bool = False,
    ) -> list[SQL]:
        """
        Build one clause per filter field.
        """

        operator = "NOT IN" if exclude else "IN"

        return [
            SQL(
                "$column " + operator + " $values",
                column=SQL.identifier(column),
                values=tuple(values),
            )
            for column, values in filters.items()
        ]

    def _where_clause(
        self,
        filters: Mapping[str, Sequence[str]],
        *,
        exclude: bool = False,
    ) -> SQL:
        """
        Build metadata filter clauses.
        """

        clauses = self._filter_clauses(filters, exclude=exclude)

        if not clauses:
            return SQL("")

        return SQL.join(
            clauses,
            SQL(" AND "),
            prefix=" WHERE ",
        )
