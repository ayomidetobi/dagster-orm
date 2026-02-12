"""Configuration for load_lookup asset using Dagster Config."""

from dagster import Config

from dagster_quickstart.orm.schema import ControlTableType, TempTableName


class LoadLookupConfig(Config):
    """Configuration for loading lookup tables to S3.

    Attributes:
        csv_path: Path to the lookup tables CSV file
        temp_table_name: Name for the temporary table
        control_table_type: Type of control table (lookup, metadata, field_map)
    """

    csv_path: str = "dagster_quickstart/data/lookup_tables.csv"
    temp_table_name: str = TempTableName.LOOKUP_TABLES.value
    control_table_type: str = ControlTableType.LOOKUP.value
