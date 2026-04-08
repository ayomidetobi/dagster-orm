"""Configuration for Hawk (Hawkeye) wide ingestion."""

from dataclasses import field
from datetime import datetime
from typing import List, Optional

from dagster import Config

from dagster_quickstart.assets.ingestion.bloomberg.config import IngestionMode
from dagster_quickstart.utils.datetime_utils import parse_datetime_string, utc_now


class HawkIngestionConfig(Config):
    """Configuration for Hawk MQL history pulls and wide Parquet writes."""

    mode: IngestionMode = IngestionMode.DAILY
    force_refresh: bool = True
    series_codes: List[str] = []
    start_date: Optional[str] = "2025-12-01"
    end_date: Optional[str] = field(
        default_factory=lambda: utc_now().strftime("%Y-%m-%d"),
    )

    def get_start_date(self) -> datetime:
        if self.start_date:
            return parse_datetime_string(self.start_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)

    def get_end_date(self) -> datetime:
        if self.end_date:
            return parse_datetime_string(self.end_date).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        return utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
