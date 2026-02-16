"""Configuration for Bloomberg data ingestion."""

from dataclasses import field
from datetime import datetime
from enum import Enum
from typing import List, Optional

from dagster import Config

from dagster_quickstart.utils.datetime_utils import parse_datetime_string, utc_now


class IngestionMode(str, Enum):
    """Ingestion mode for Bloomberg data."""

    DAILY = "daily"  # Uses series_code from partition or fetches from database if not provided
    BACKFILL = "backfill"  # Uses series_codes from config for backfilling historical data


class BloombergIngestionConfig(Config):
    """Configuration for Bloomberg data ingestion."""

    mode: IngestionMode = (
        IngestionMode.DAILY  # IngestionMode.BACKFILL uses series_codes from config for backfilling historical data
    )
    force_refresh: bool = False  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running a partition). If False, skip insertion if data already exists for the date.
    use_dummy_data: bool = True  # If True, use dummy  data instead of real database queries. Useful for testing with PyPDL.
    series_codes: List[
        str
    ] = []  # List of series codes for backfill ingestion (required when mode=IngestionMode.BACKFILL)
    start_date: Optional[str] = field(
        default_factory=lambda: utc_now().strftime("%Y-%m-%d"),
    )

    end_date: Optional[str] = field(
        default_factory=lambda: utc_now().strftime("%Y-%m-%d"),
    )

    def get_start_date(self) -> datetime:
        """Get start date as datetime object.

        Uses utc_now() as default if start_date is not provided.

        Returns:
            Start date as datetime object, or current UTC date from utc_now() if start_date is None
        """
        if self.start_date:
            return parse_datetime_string(self.start_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        # Default to current UTC date
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)

    def get_end_date(self) -> datetime:
        """Get end date as datetime object.

        Uses utc_now() as default if end_date is not provided.

        Returns:
            End date as datetime object, or current UTC date from utc_now() if end_date is None
        """
        if self.end_date:
            return parse_datetime_string(self.end_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        # Default to current UTC date
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)


class BloombergBackfillIngestionConfig(Config):
    """Configuration for backfill Bloomberg data ingestion (date range in config)."""

    mode: IngestionMode = (
        IngestionMode.BACKFILL  # IngestionMode.BACKFILL uses series_codes from config for backfilling historical data
    )
    force_refresh: bool = True  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running a partition). If False, skip insertion if data already exists for the date.
    use_dummy_data: bool = True  # If True, use dummy  data instead of real database queries. Useful for testing with PyPDL.
    series_codes: List[
        str
    ] = []  # List of series codes for backfill ingestion (required when mode=IngestionMode.BACKFILL)
    start_date: Optional[str] = field(default_factory=lambda: utc_now().strftime("%Y-%m-%d"))

    end_date: Optional[str] = field(
        default_factory=lambda: utc_now().strftime("%Y-%m-%d"),
    )

    def get_start_date(self) -> datetime:
        """Get start date as datetime object.

        Uses utc_now() as default if start_date is not provided.

        Returns:
            Start date as datetime object, or current UTC date from utc_now() if start_date is None
        """
        if self.start_date:
            return parse_datetime_string(self.start_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        # Default to current UTC date
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)

    def get_end_date(self) -> datetime:
        """Get end date as datetime object.

        Uses utc_now() as default if end_date is not provided.

        Returns:
            End date as datetime object, or current UTC date from utc_now() if end_date is None
        """
        if self.end_date:
            return parse_datetime_string(self.end_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        # Default to current UTC date
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
