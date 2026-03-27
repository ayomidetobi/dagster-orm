"""Configuration for load_series_dependencies asset using Dagster Config."""

from dagster import Config

from dagster_quickstart.orm.schema import ControlTableType, TempTableName


class LoadSeriesDependenciesConfig(Config):
    """Configuration for loading series dependencies to S3.

    Attributes:
        csv_path: Path to the series dependencies CSV file
        temp_table_name: Name for the temporary table
        control_table_type: Type of control table (default: metadata_derived)
    """

    csv_path: str = "dagster_quickstart/data/series_dependencies.csv"
    temp_table_name: str = TempTableName.METADATA_DERIVED.value
    control_table_type: str = ControlTableType.METADATA_DERIVED.value
