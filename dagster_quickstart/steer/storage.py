"""Silver/gold DuckLake storage for the STEER pipeline.

DuckLake today (rewrite/data_api/) is flat -- just metadata/values/
metadata_derived tables, no bronze/silver/gold schema convention. This adds
silver/gold as real DuckDB schemas inside the *same* DuckLake catalog
(confirmed live: CREATE SCHEMA/CREATE TABLE/INSERT/SELECT all work against
the attached catalog) without touching the existing ingestion tables at
all -- those stay exactly as they are and are treated as the de facto
bronze layer.

Uses its own DuckLake attach (via rewrite.data_api.bootstrap's
build_default_connection() -- attach only, no repository/schema-init)
rather than reaching into RewriteDataAPIResource's private connection or
build_default_container()'s full DataAPI stack -- keeps this layer
decoupled from the ingestion DataAPI, at the cost of one extra Postgres+S3
attach per run (a few seconds; this is a once-daily batch job, not a hot
path). Using build_default_connection() specifically (not
build_default_container()) matters: the latter also runs
DuckLakeValueStorageRepository.initialize_schema() (schema-altering DDL on
the `values` table), and a job that needs both `rewrite_data_api` and
`steer_catalog` resources initializes both in the same run -- two
connections both running that DDL concurrently is a real DuckLake
transaction conflict (confirmed: this crashed with "Transaction conflict --
attempting to alter table ... but another transaction has altered it"
before this was fixed to use the connection-only path here).
"""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.bootstrap import build_default_connection

logger = structlog.get_logger(__name__)

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

STEER_ESTIMATES_TABLE = "steer_estimates"
STEER_SIGNALS_TABLE = "steer_signals"
#: SteerResult's 2 tables -- see steer/results.py's module docstring.
#: steer_results is long-form (one row per series_code/as_of/date);
#: steer_result_summary is one row per series_code/as_of (z_score,
#: upper/lower, and every coefficient/standard_error/p_value, flattened).
STEER_RESULTS_TABLE = "steer_results"
STEER_RESULT_SUMMARY_TABLE = "steer_result_summary"


class SteerCatalog:
    """Thin wrapper over a DuckLake connection for the STEER silver/gold tables.

    Owns its own DuckLake attach (see build()) -- one per resource
    lifetime, not per call. `frame` is written with `INSERT ... SELECT *
    FROM frame` (DuckDB can query a local pandas variable directly), which
    is itself a DuckLake snapshot -- see rewrite/data_api's own writers for
    the same append-only convention. This module never deletes/updates a
    row; a re-run for the same partition appends a new snapshot on top,
    exactly like the rest of DuckLake here.
    """

    def __init__(self, connection) -> None:
        self._connection = connection

    @classmethod
    def build(cls) -> "SteerCatalog":
        """Attach a fresh DuckLake connection (same catalog/credentials as the rest of the app)."""
        return cls(build_default_connection())

    def ensure_schemas(self) -> None:
        """Create the silver/gold schemas if they don't exist yet. Idempotent."""
        self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
        self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

    def ensure_table(self, schema: str, table: str, frame: pd.DataFrame) -> None:
        """Create `schema.table` (from `frame`'s columns/dtypes) if it doesn't exist yet."""
        self._connection.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS SELECT * FROM frame LIMIT 0"
        )

    def _table_exists(self, schema: str, table: str) -> bool:
        return bool(
            self._connection.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            ).fetchone()[0]
        )

    def write(self, schema: str, table: str, frame: pd.DataFrame) -> None:
        """Append `frame` to `schema.table` (creating the table on first write).

        Different callers write different column sets to the same table
        name -- e.g. gold.steer_result_summary gets G10's 5-driver
        coefficient columns and CHN's 7-driver ones, all keyed off whatever
        StrategyConfig.drivers that pair's universe has. A plain positional
        `INSERT ... SELECT *` would let whichever write created the table
        fix its column set/order, silently misaligning or erroring on every
        later write with a different one.
        `... UNION ALL BY NAME ...` (name-based, not positional) instead:
        the existing table's rows and the new frame's rows are unioned by
        column name, so a column present in one side and absent in the
        other is simply padded with NULL rather than misaligned -- and the
        table widens automatically the first time a wider driver set (e.g.
        CHN's) is written, whichever universe happened to write first.
        """
        if frame.empty:
            logger.info("steer_catalog_write_skipped_empty", schema=schema, table=table)
            return
        self.ensure_schemas()
        if not self._table_exists(schema, table):
            self.ensure_table(schema, table, frame)
            self._connection.execute(f"INSERT INTO {schema}.{table} SELECT * FROM frame")
        else:
            self._connection.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table} AS "
                f"SELECT * FROM {schema}.{table} UNION ALL BY NAME SELECT * FROM frame"
            )
        logger.info("steer_catalog_write", schema=schema, table=table, row_count=len(frame))

    def read(
        self,
        schema: str,
        table: str,
        *,
        universe: Optional[str] = None,
        series_codes: Optional[Iterable[str]] = None,
    ) -> pd.DataFrame:
        """Read `schema.table`, optionally filtered by universe/series_code. Empty frame if the table doesn't exist yet."""
        if not self._table_exists(schema, table):
            return pd.DataFrame()

        clauses, params = [], []
        if universe is not None:
            clauses.append("universe = ?")
            params.append(universe)
        if series_codes is not None:
            codes = list(series_codes)
            clauses.append(f"series_code IN ({', '.join('?' for _ in codes)})")
            params.extend(codes)

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._connection.execute(f"SELECT * FROM {schema}.{table}{where}", params).fetchdf()
