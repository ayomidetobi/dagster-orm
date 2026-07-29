"""Configuration for the DuckLake-native Bloomberg values ingestion asset."""

from typing import List, Optional

from dagster import Config


class BloombergValuesConfig(Config):
    """Configuration for ingest_bloomberg_values.

    Attributes:
        series_codes: Explicit series to fetch. Empty (default) means every
            metadata series that has a Bloomberg ticker (bbg_ticker populated).
        start: ISO date string bounding the fetch window (e.g. "2024-01-01").
            None lets the vendor client use its own default.
        end: ISO date string bounding the fetch window. None lets the vendor
            client use its own default (typically "now").
    """

    series_codes: List[str] = []
    start: Optional[str] = None
    end: Optional[str] = None
