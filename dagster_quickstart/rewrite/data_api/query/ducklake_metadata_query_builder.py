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

    def build_ensure_table(self, relation: str, columns: Sequence[str]) -> SQL:
        """
        Create the table (with these columns) if it doesn't exist yet.

        Metadata columns are meant to stay flexible (arbitrary per-asset-class
        attributes), so which columns exist is inferred from the first write
        rather than declared up front -- but every column is explicitly cast
        to VARCHAR rather than left to DuckDB's type inference from the
        sample relation. Left uncast, a column that's entirely NULL in the
        first-ever write (e.g. a derived series saved before parent_series_code
        is populated) infers a numeric SQL type; any later write with real
        text in that column (e.g. a pipe-delimited parent_series_code list)
        then fails with a ConversionException.
        """

        select_list = SQL.join(
            [
                SQL("$column::VARCHAR AS $column", column=SQL.identifier(column))
                for column in columns
            ],
            SQL(", "),
        )

        return SQL(
            "CREATE TABLE IF NOT EXISTS $table AS SELECT $select_list FROM $relation LIMIT 0",
            table=SQL.identifier(self._table),
            select_list=select_list,
            relation=SQL.identifier(relation),
        )

    def build_save_metadata(
        self,
        relation: str,
        *,
        table_columns: Sequence[str],
        frame_columns: Sequence[str],
    ) -> SQL:
        """
        Build an append-only INSERT statement.

        DuckLake snapshots every write, so history is preserved without a
        manual upsert/merge strategy. Call build_ensure_table() first so the
        table exists.

        Inserts by column name against every column the table currently
        has (table_columns), not just the ones in this particular frame
        (frame_columns) -- metadata columns vary per asset class, so a
        later save with fewer/different columns than an earlier one must
        NULL-fill the table's other columns rather than break on a
        `SELECT *` position/count mismatch. Callers add any brand-new
        columns (in frame_columns but not yet in table_columns) via
        build_add_column() before calling this.
        """

        frame_column_set = set(frame_columns)

        insert_columns = SQL.join(
            [SQL("$column", column=SQL.identifier(column)) for column in table_columns],
            SQL(", "),
        )
        select_columns = SQL.join(
            [
                SQL("$column", column=SQL.identifier(column))
                if column in frame_column_set
                else SQL("NULL AS $column", column=SQL.identifier(column))
                for column in table_columns
            ],
            SQL(", "),
        )

        return SQL(
            "INSERT INTO $table ($insert_columns) SELECT $select_columns FROM $relation",
            table=SQL.identifier(self._table),
            insert_columns=insert_columns,
            select_columns=select_columns,
            relation=SQL.identifier(relation),
        )

    def build_add_column(self, column: str) -> SQL:
        """
        Build ALTER TABLE ... ADD COLUMN for a brand-new metadata attribute.

        Metadata columns are intentionally flexible per asset class -- a
        save introducing a column the table doesn't have yet gets it added
        (as a nullable VARCHAR) rather than silently dropped or erroring.
        """

        return SQL(
            "ALTER TABLE $table ADD COLUMN IF NOT EXISTS $column VARCHAR",
            table=SQL.identifier(self._table),
            column=SQL.identifier(column),
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
