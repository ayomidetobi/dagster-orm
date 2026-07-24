"""DuckLake implementation of the MetadataStorageRepository."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

import duckdb
import pandas as pd
import structlog

from rewrite.data_api.columns import ControlTables, MetadataColumns
from rewrite.data_api.repositories.base_ducklake_repository import BaseDuckLakeRepository
from rewrite.data_api.repositories.storage_repository import MetadataStorageRepository
from rewrite.data_api.query.ducklake_metadata_query_builder import (
    DuckLakeMetadataQueryBuilder,
)

logger = structlog.get_logger(__name__)


class DuckLakeMetadataStorageRepository(
    BaseDuckLakeRepository,
    MetadataStorageRepository,
):
    """
    DuckLake implementation of metadata storage.

    This repository is responsible only for orchestrating storage
    operations. SQL generation is delegated to DuckLakeMetadataQueryBuilder,
    while transaction management and SQL execution are inherited from
    BaseDuckLakeRepository.
    """

    DEFAULT_TABLE = ControlTables.METADATA

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

        self._builder = DuckLakeMetadataQueryBuilder(
            table_name=self._table,
        )

    def get_metadata(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return metadata rows matching the requested filters."""

        sql = self._builder.build_get_metadata(
            filters=filters,
            exclude=exclude,
            version=version,
            as_of=as_of,
        )

        return self.execute(sql)

    def get_columns(self) -> list[str]:
        """Return the available column names, for filter validation/discovery.

        Returns an empty list if the table hasn't been created yet (nothing
        ingested), rather than raising -- that's a legitimate "no columns
        yet" state, not an error.
        """

        try:
            return list(self.execute(self._builder.build_columns()).columns)
        except Exception:
            logger.debug("ducklake_metadata_columns_unavailable", table=self._table)
            return []

    def get_distinct_values(
        self,
        column: str,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
    ) -> list[str]:
        """Return the distinct, non-null values for a column, optionally filtered.

        Computed by the database (SELECT DISTINCT) rather than pulling the
        whole table into pandas.
        """

        sql = self._builder.build_distinct_values(column, filters=filters, exclude=exclude)
        df = self.execute(sql)

        if df.empty:
            return []

        return [value for value in df["value"].astype(str).str.strip() if value]

    def save_metadata(self, frame: pd.DataFrame, *, fresh: bool = False) -> None:
        """Persist normalized metadata rows.

        Metadata columns vary per asset class, so a save whose columns
        don't exactly match the table's existing columns is reconciled
        rather than left to break: columns the table already has that
        this frame lacks are NULL-filled, and columns this frame has that
        the table lacks are added to the table first.

        fresh=True deletes any existing rows whose series_code matches one
        in this frame before inserting, so re-saving the same series
        replaces rather than accumulates duplicates -- series_codes from
        other, previously-saved frames are untouched. fresh=False (default)
        just appends, exactly as before. Deletion and insertion happen in
        the same transaction, so a failed insert can't leave the table
        without those rows.
        """

        if frame.empty:
            return

        logger.info("ducklake_metadata_save", table=self._table, row_count=len(frame), fresh=fresh)

        with self.transaction():
            with self.register_dataframe(frame) as relation:
                frame_columns = list(frame.columns)
                self.execute_no_result(self._builder.build_ensure_table(relation, frame_columns))

                if fresh and MetadataColumns.SERIES_CODE in frame.columns:
                    series_codes = (
                        frame[MetadataColumns.SERIES_CODE].dropna().astype(str).str.strip()
                    )
                    series_codes = [code for code in dict.fromkeys(series_codes.tolist()) if code]
                    if series_codes:
                        self.execute_no_result(
                            self._builder.build_delete({MetadataColumns.SERIES_CODE: series_codes})
                        )

                table_columns = self.get_columns()
                new_columns = [column for column in frame_columns if column not in table_columns]
                for column in new_columns:
                    self.execute_no_result(self._builder.build_add_column(column))
                table_columns = table_columns + new_columns

                self.execute_no_result(
                    self._builder.build_save_metadata(
                        relation,
                        table_columns=table_columns,
                        frame_columns=frame_columns,
                    )
                )

    def refresh_metadata(self) -> None:
        """
        Refresh catalog state.

        Currently a no-op because DuckLake reflects committed
        transactions immediately.
        """
        return

    def initialize_schema(self) -> None:
        """
        No-op: metadata's schema is inferred from the first save_metadata()
        call (columns stay flexible per asset class), so there's nothing to
        create ahead of time.
        """
        return
