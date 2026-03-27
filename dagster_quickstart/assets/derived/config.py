"""Configuration for derived series calculation asset using Dagster Config."""

from dataclasses import field
from typing import Optional

from dagster import Config

from dagster_quickstart.orm.schema import ControlTableType
from dagster_quickstart.utils.datetime_utils import utc_now


class DerivedConfig(Config):
    """Configuration for calculating derived series.

    Attributes:
        start_date: Start date for calculation (inclusive)
        end_date: End date for calculation (inclusive), defaults to current date if not provided
        control_table_type: Type of control table (default: metadata_derived)
    """

    start_date: Optional[str] = "2025-12-01"
    end_date: Optional[str] = field(
        default_factory=lambda: utc_now().strftime("%Y-%m-%d"),
    )  # Date string in format YYYY-MM-DD, defaults to current date
    control_table_type: str = ControlTableType.METADATA_DERIVED.value
