"""Storage repository for arbitrary DuckLake tables with no fixed shape.

For tables outside the metadata/values schema this package's other repositories validate
against -- STEER's silver/gold tables, for instance (see DataAPI.read_table()/write_table()).
Shares the same DuckLake connection every other repository does -- built from the same
`duckdb_connection` dependency the rewrite container already declares (see container.py) --
so a caller that needs this alongside the metadata/value repositories never attaches a second
connection.
"""

from __future__ import annotations

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.repositories.base_ducklake_repository import BaseDuckLakeRepository

logger = structlog.get_logger(__name__)


class GenericTableRepository(BaseDuckLakeRepository):
    """Append-only read/write for one `schema.table`, widening on write instead of rewriting.

    write() never rewrites an existing row: a frame introducing a column the table doesn't
    have yet widens the table first (ALTER TABLE ADD COLUMN, typed from whatever DuckDB itself
    infers for that column from the incoming frame -- see write()), then every write is a plain
    `INSERT INTO table (explicit, column, list) SELECT ...` -- never `CREATE OR REPLACE TABLE`
    or `UNION ALL BY NAME`, which would re-read and re-materialise every row written before it.
    A column the table already has that this particular frame lacks is NULL-padded in the
    INSERT's SELECT list (by explicit name, never position), rather than causing a mismatch --
    e.g. a G10 write (5 driver columns) after a CHN write (7) to the same table. Also creates
    `schema` (CREATE SCHEMA IF NOT EXISTS) on first write, so a caller never has to provision
    the schema itself before writing to it.
    """

    def table_exists(self, schema: str, table: str) -> bool:
        result = self.execute(
            "SELECT count(*) AS n FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        )
        return bool(result["n"].iloc[0])

    def get_columns(self, schema: str, table: str) -> list[str]:
        """Column names currently in `schema.table`. Empty list if it doesn't exist yet."""
        if not self.table_exists(schema, table):
            return []
        return list(self.execute(f"SELECT * FROM {self._qualify(schema, table)} LIMIT 0").columns)

    def read_all(self, schema: str, table: str) -> pd.DataFrame:
        """Every row of `schema.table`. Empty frame if it doesn't exist yet."""
        if not self.table_exists(schema, table):
            return pd.DataFrame()
        return self.execute(f"SELECT * FROM {self._qualify(schema, table)}")

    def write(self, schema: str, table: str, frame: pd.DataFrame) -> None:
        """Append `frame` to `schema.table`, creating/widening it as needed. See class docstring."""
        if frame.empty:
            logger.info("generic_table_write_skipped_empty", schema=schema, table=table)
            return

        qualified = self._qualify(schema, table)
        frame_columns = list(frame.columns)

        with self.transaction():
            self.execute_no_result(f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema)}")
            with self.register_dataframe(frame) as relation:
                if not self.table_exists(schema, table):
                    self.execute_no_result(
                        f"CREATE TABLE {qualified} AS SELECT * FROM {relation} LIMIT 0"
                    )

                existing_columns = self.get_columns(schema, table)
                existing_lower = {column.lower() for column in existing_columns}
                new_columns = [
                    column for column in frame_columns if column.lower() not in existing_lower
                ]

                if new_columns:
                    described = self.execute(f"DESCRIBE SELECT * FROM {relation}")
                    duckdb_type_by_column = dict(
                        zip(described["column_name"], described["column_type"])
                    )
                    for column in new_columns:
                        self.execute_no_result(
                            f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS "
                            f"{self.quote_identifier(column)} {duckdb_type_by_column[column]}"
                        )
                    existing_columns = existing_columns + new_columns

                frame_column_set = set(frame_columns)
                insert_columns = ", ".join(self.quote_identifier(c) for c in existing_columns)
                select_columns = ", ".join(
                    self.quote_identifier(c) if c in frame_column_set
                    else f"NULL AS {self.quote_identifier(c)}"
                    for c in existing_columns
                )
                self.execute_no_result(
                    f"INSERT INTO {qualified} ({insert_columns}) "
                    f"SELECT {select_columns} FROM {relation}"
                )

        logger.info("generic_table_write", schema=schema, table=table, row_count=len(frame))

    def _qualify(self, schema: str, table: str) -> str:
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"
