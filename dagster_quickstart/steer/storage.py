"""DuckLake silver/gold schema and table names for the STEER pipeline.

Silver/gold aren't a bronze/silver/gold convention DuckLake (rewrite/data_api/) has any notion
of on its own -- just metadata/values/metadata_derived tables. STEER adds silver/gold as real
DuckDB schemas inside the *same* DuckLake catalog the rest of the app already uses, via
DataAPI.read_table()/.write_table() (rewrite.data_api.api.data_api) -- see
rewrite.data_api.repositories.generic_table_repository.GenericTableRepository for how those
read/write without a second DuckLake attach and without ever rewriting an existing row. This
module only holds the schema/table names every caller needs to agree on; it owns no connection
and no read/write logic itself any more.
"""

from __future__ import annotations

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
