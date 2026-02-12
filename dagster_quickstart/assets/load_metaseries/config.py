"""Configuration for load_metaseries asset using Dagster Config."""

from typing import List

from dagster import Config

from dagster_quickstart.orm.schema import (
    ControlTableType,
    PreviewColumns,
    TempTableName,
)


class LoadMetaSeriesConfig(Config):
    """Configuration for loading meta series to S3.

    Attributes:
        csv_path: Path to the meta series CSV file
        temp_table_name: Name for the temporary table
        control_table_type: Type of control table (default: metadata)
        preview_limit: Number of rows to include in preview
        preview_columns: List of column names for preview
    """

    csv_path: str = "dagster_quickstart/data/meta_series.csv"
    temp_table_name: str = TempTableName.META_SERIES.value
    control_table_type: str = ControlTableType.METADATA.value
    preview_limit: int = 10
    preview_columns: List[str] = PreviewColumns.get_default_columns()
