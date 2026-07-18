"""SQL builder for DuckLake value storage."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from resources.duckdb_datacacher import SQL
from rewrite.data_api.columns import ValueColumns
from rewrite.data_api.query.snapshot import table_reference


class DuckLakeValueQueryBuilder:
    """
    Builds SQL statements for DuckLake value storage.

    This class is responsible only for generating SQL.

    It never executes SQL, manages transactions, or interacts with
    DuckDB connections.
    """

    def __init__(
        self,
        *,
        table_name: str,
    ) -> None:
        self._table = table_name

    def build_get_values(
        self,
        *,
        series_codes: Sequence[str],
        ticker_source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        order_by: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> SQL:
        """Build SQL for retrieving value rows."""

        sql = SQL(
            """
            SELECT *
            FROM $table
            """,
            table=table_reference(self._table, version=version, as_of=as_of),
        )

        sql += self._where_clause(
            series_codes=series_codes,
            ticker_source=ticker_source,
            start=start,
            end=end,
        )

        if order_by:
            direction = "ASC" if ascending else "DESC"
            sql += SQL(
                f" ORDER BY $column {direction}",
                column=SQL.identifier(order_by),
            )

        if limit is not None:
            sql += SQL(
                " LIMIT $limit",
                limit=limit,
            )

        return sql

    def build_get_last_values(
        self,
        *,
        series_codes: Sequence[str],
        ticker_source: str | None = None,
        latest_non_null: bool = True,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> SQL:
        """Build SQL returning the latest value per series."""

        sql = SQL(
            f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {ValueColumns.SERIES_CODE}
                           ORDER BY {ValueColumns.TIMESTAMP} DESC
                       ) AS rn
                FROM $table
            """,
            table=table_reference(self._table, version=version, as_of=as_of),
        )

        sql += self._where_clause(
            series_codes=series_codes,
            ticker_source=ticker_source,
            latest_non_null=latest_non_null,
        )

        sql += SQL(
            """
            )
            WHERE rn = 1
            """
        )

        return sql

    def build_exists(
        self,
        *,
        series_codes: Sequence[str],
        ticker_source: str | None = None,
    ) -> SQL:
        """Build SQL checking whether series contain values."""

        sql = SQL(
            f"""
            SELECT
                {ValueColumns.SERIES_CODE},
                COUNT(*) > 0 AS exists
            FROM $table
            """,
            table=SQL.identifier(self._table),
        )

        sql += self._where_clause(
            series_codes=series_codes,
            ticker_source=ticker_source,
        )

        sql += SQL(
            f"""
            GROUP BY {ValueColumns.SERIES_CODE}
            """
        )

        return sql

    def build_delete(
        self,
        filters: Mapping[str, object],
    ) -> SQL:
        """Build DELETE statement."""

        sql = SQL(
            """
            DELETE FROM $table
            """,
            table=SQL.identifier(self._table),
        )

        clauses = []

        for column, value in filters.items():
            clauses.append(
                SQL(
                    "$column = $value",
                    column=SQL.identifier(column),
                    value=value,
                )
            )

        if clauses:
            sql += SQL.join(clauses, SQL(" AND "), prefix=" WHERE ")

        return sql

    def build_save(
        self,
        relation: str,
    ) -> SQL:
        """
        Build an append-only INSERT statement.

        DuckLake snapshots every write, so history is preserved without a
        manual upsert/merge strategy. Inserts by column name rather than
        `SELECT *` -- a positional insert would silently misalign columns
        (or hard-fail on a count mismatch) whenever the source relation's
        columns aren't in exactly the table's physical order. The caller
        (DuckLakeValueStorageRepository.save_values()) guarantees the
        relation has every one of these columns first.
        """

        columns = ", ".join(
            (
                ValueColumns.SERIES_CODE,
                ValueColumns.TIMESTAMP,
                ValueColumns.VALUE,
                ValueColumns.TICKER_SOURCE,
            )
        )

        return SQL(
            f"INSERT INTO $table ({columns}) SELECT {columns} FROM $relation",
            table=SQL.identifier(self._table),
            relation=SQL.identifier(relation),
        )

    def _where_clause(
        self,
        *,
        series_codes: Sequence[str] | None = None,
        ticker_source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        latest_non_null: bool = False,
    ) -> SQL:
        """Construct a reusable WHERE clause."""

        clauses: list[SQL] = []

        if series_codes:
            clauses.append(
                SQL(
                    f"{ValueColumns.SERIES_CODE} IN $codes",
                    codes=tuple(series_codes),
                )
            )

        if ticker_source:
            clauses.append(
                SQL(
                    f"{ValueColumns.TICKER_SOURCE} = $source",
                    source=ticker_source,
                )
            )

        if start:
            clauses.append(
                SQL(
                    f"{ValueColumns.TIMESTAMP} >= $start",
                    start=start,
                )
            )

        if end:
            clauses.append(
                SQL(
                    f"{ValueColumns.TIMESTAMP} <= $end",
                    end=end,
                )
            )

        if latest_non_null:
            clauses.append(SQL(f"{ValueColumns.VALUE} IS NOT NULL"))

        if not clauses:
            return SQL("")

        return SQL.join(
            clauses,
            SQL(" AND "),
            prefix=" WHERE ",
        )
