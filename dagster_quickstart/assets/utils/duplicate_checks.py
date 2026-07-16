"""Shared helpers for duplicate key validation in asset checks."""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from dagster import AssetCheckExecutionContext


def find_duplicate_keys(
    duckdb_repo: Any,
    parquet_source: str,
    column: str,
    alias: str | None = None,
) -> "pd.DataFrame":
    """Return dataframe of duplicate key values for the given column.

    Args:
        duckdb_repo: Repository with execute_raw_sql method.
        parquet_source: DuckDB parquet source expression.
        column: Column name to check for duplicates.
        alias: Optional alias for the selected column name.

    Returns:
        DataFrame with key column and duplicate_count column.
    """
    key_alias = alias or column
    duplicate_sql = f"""
    SELECT
        {column} AS {key_alias},
        COUNT(*) AS duplicate_count
    FROM {parquet_source}
    GROUP BY {column}
    HAVING COUNT(*) > 1
    """
    return duckdb_repo.execute_raw_sql(duplicate_sql)


def log_duplicate_errors(
    context: "AssetCheckExecutionContext",
    duplicate_df: "pd.DataFrame",
    key_column: str,
    location_label: str,
) -> None:
    """Log duplicate key errors for an asset check.

    Args:
        context: Dagster asset check execution context.
        duplicate_df: DataFrame returned by find_duplicate_keys.
        key_column: Name of the key column in duplicate_df.
        location_label: Human-readable label describing where duplicates were found.
    """
    for _, row in duplicate_df.iterrows():
        context.log.error(
            f"Duplicate {key_column} {row[key_column]} appears "
            f"{int(row['duplicate_count'])} time(s) in {location_label}"
        )
