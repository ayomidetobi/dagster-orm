"""Configuration for load_metaseries asset using Dagster Config."""

from typing import List

from dagster import Config

DEFAULT_PREVIEW_COLUMNS = ["series_code", "series_name", "asset_class"]


class LoadMetaSeriesConfig(Config):
    """Configuration for loading meta series into the rewrite DuckLake catalog.

    Attributes:
        csv_path: Path to the meta series CSV file
        fresh: Replace existing rows for this file's series_codes instead of
            appending alongside them (see rewrite DataAPI.import_metadata).
            Defaults to True so re-materializing this asset stays idempotent,
            matching the old asset's overwrite-on-every-run behavior.
        preview_limit: Number of rows to include in preview
        preview_columns: List of column names for preview
    """

    csv_path: str = "dagster_quickstart/data/meta_series.csv"
    fresh: bool = True
    preview_limit: int = 10
    preview_columns: List[str] = DEFAULT_PREVIEW_COLUMNS
