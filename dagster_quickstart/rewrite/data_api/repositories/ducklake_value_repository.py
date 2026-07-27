"""DuckLake implementation of the ValueStorageRepository."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

import duckdb
import pandas as pd
import structlog

from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.rewrite.data_api.columns import ControlTables, TickerSource, ValueColumns
from dagster_quickstart.rewrite.data_api.repositories.base_ducklake_repository import BaseDuckLakeRepository
from dagster_quickstart.rewrite.data_api.repositories.storage_repository import ValueStorageRepository
from dagster_quickstart.rewrite.data_api.query.ducklake_query import (
    DuckLakeValueQueryBuilder,
)

logger = structlog.get_logger(__name__)


class DuckLakeValueStorageRepository(
    BaseDuckLakeRepository,
    ValueStorageRepository,
):
    """
    DuckLake implementation of value storage.

    This repository is responsible only for orchestrating storage
    operations. SQL generation is delegated to DuckLakeValueQueryBuilder,
    while transaction management and SQL execution are inherited from
    BaseDuckLakeRepository.
    """

    DEFAULT_TABLE = ControlTables.VALUES

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        table_name: str = DEFAULT_TABLE,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> None:
        super().__init__(
            connection,
            catalog=catalog,
            schema=schema,
        )

        self._table = self.qualify_table(table_name)

        self._builder = DuckLakeValueQueryBuilder(
            table_name=self._table,
        )

    def get_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        order_by: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return values for the requested series."""

        sql = self._builder.build_get_values(
            series_codes=series_codes,
            ticker_source=ticker_source,
            start=start,
            end=end,
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            version=version,
            as_of=as_of,
        )

        return self.execute(sql)

    def get_last_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        latest_non_null: bool = True,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return the latest value row for each requested series."""

        sql = self._builder.build_get_last_values(
            series_codes=series_codes,
            ticker_source=ticker_source,
            latest_non_null=latest_non_null,
            version=version,
            as_of=as_of,
        )

        return self.execute(sql)

    def value_exists(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
    ) -> Mapping[str, bool]:
        """Check whether values exist for the requested series."""

        sql = self._builder.build_exists(
            series_codes=series_codes,
            ticker_source=ticker_source,
        )

        df = self.execute(sql)

        return dict(zip(df[ValueColumns.SERIES_CODE], df["exists"]))

    def save_values(
        self,
        frame: pd.DataFrame,
    ) -> None:
        """Persist value rows."""

        if frame.empty:
            return

        logger.info("ducklake_value_save", table=self._table, row_count=len(frame))

        with self.transaction():
            with self.register_dataframe(self._with_all_value_columns(frame)) as relation:
                sql = self._builder.build_save(
                    relation,
                )

                self.execute_no_result(sql)

    @staticmethod
    def _with_all_value_columns(frame: pd.DataFrame) -> pd.DataFrame:
        """Add any of the table's physical columns the frame is missing.

        build_save() inserts by column name against all four physical
        columns (series_code/timestamp/value/ticker_source) -- e.g.
        ticker_source is optional on the incoming frame (not every caller
        tags a source), so it's backfilled here rather than left to break
        the insert.

        ticker_source specifically is never left NULL (missing column, or
        present but with null cells): the table is partitioned by it, and
        DuckLake crashes (InternalException: "StringValue::Get on a NULL
        value") if a NULL lands in a partitioned column. TickerSource.UNKNOWN
        is used instead -- a real, queryable value rather than a silent gap.
        """

        frame = frame.copy()

        for column in (ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE):
            if column not in frame.columns:
                frame[column] = None

        if ValueColumns.TICKER_SOURCE not in frame.columns:
            frame[ValueColumns.TICKER_SOURCE] = TickerSource.UNKNOWN
        else:
            frame[ValueColumns.TICKER_SOURCE] = frame[ValueColumns.TICKER_SOURCE].fillna(
                TickerSource.UNKNOWN
            )

        return frame

    def delete_values(
        self,
        filters: Mapping[str, object],
    ) -> None:
        """Delete value rows matching the supplied filters."""

        logger.info(
            "ducklake_value_delete", table=self._table, filter_fields=sorted(filters.keys())
        )

        with self.transaction():
            sql = self._builder.build_delete(filters)

            self.execute_no_result(sql)

    def initialize_schema(self) -> None:
        """Create the values table if it doesn't already exist.

        Partitioned by ticker_source and year(timestamp) -- see
        DuckLakeValueQueryBuilder.build_set_partitioned_by().
        """

        logger.info("ducklake_value_initialize_schema", table=self._table)

        self.execute_no_result(
            SQL(
                "CREATE TABLE IF NOT EXISTS $table "
                f"({ValueColumns.SERIES_CODE} VARCHAR, {ValueColumns.TIMESTAMP} TIMESTAMP, "
                f"{ValueColumns.VALUE} DOUBLE, {ValueColumns.TICKER_SOURCE} VARCHAR)",
                table=SQL.identifier(self._table),
            )
        )
        self.execute_no_result(self._builder.build_set_partitioned_by())
