"""Configuration for derived series calculation asset using Dagster Config."""

from dagster import Config

from dagster_quickstart.orm.schema import ControlTableType


class DerivedConfig(Config):
    """Configuration for calculating derived series.

    Attributes:
        start_date: Start date for calculation (inclusive)
        end_date: End date for calculation (inclusive)
        control_table_type: Type of control table (default: series_dependencies)
    """

    start_date: str  # Date string in format YYYY-MM-DD
    end_date: str  # Date string in format YYYY-MM-DD
    control_table_type: str = ControlTableType.SERIES_DEPENDENCIES.value
