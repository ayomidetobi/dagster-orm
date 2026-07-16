"""Named constants for column names, control tables, and vendor identifiers.

Centralizes the string literals used across repositories, query builders,
vendors, and services so a typo becomes an ImportError/AttributeError at
import time instead of a silent runtime mismatch.
"""

from __future__ import annotations


class ValueColumns:
    """Column names for the values table and long-form value frames."""

    SERIES_CODE = "series_code"
    TIMESTAMP = "timestamp"
    VALUE = "value"
    TICKER_SOURCE = "ticker_source"


class MetadataColumns:
    """Column names used in metadata / metadata_derived frames."""

    SERIES_CODE = "series_code"
    SERIES_NAME = "series_name"
    PARENT_SERIES_CODE = "parent_series_code"
    CALC_TYPE = "calc_type"


class ControlTables:
    """DuckLake table names for the metadata/value control tables."""

    METADATA = "metadata"
    METADATA_DERIVED = "metadata_derived"
    VALUES = "values"


class TickerSource:
    """Supported vendor ticker sources."""

    BLOOMBERG = "bloomberg"
    HAWK = "hawk"
    MDS = "mds"

    ALL = (BLOOMBERG, HAWK, MDS)


TICKER_SOURCE_ALIASES: dict[str, str] = {
    "BBG": TickerSource.BLOOMBERG,
    "BLOOMBERG": TickerSource.BLOOMBERG,
    "HAWK": TickerSource.HAWK,
    "HAWKEYE": TickerSource.HAWK,
    "MDS": TickerSource.MDS,
    "ONETICK": TickerSource.MDS,
}


def normalize_ticker_source(value: str) -> str:
    """Resolve a vendor abbreviation/alias (case-insensitive) to its canonical name.

    e.g. "BBG", "Bloomberg", "bloomberg" all resolve to TickerSource.BLOOMBERG.
    Falls back to a lowercased copy of the input when it isn't a known alias,
    so unrecognized sources still fail later with a clear
    UnsupportedTickerSourceError/UnsupportedVendorError instead of silently
    here.
    """

    key = value.strip().upper()
    return TICKER_SOURCE_ALIASES.get(key, value.strip().lower())


class CalcType:
    """Derived-series calculator types."""

    SPREAD = "SPREAD"
    FLY = "FLY"
    BOX = "BOX"
    RATIO = "RATIO"
    SPREAD_INV = "SPREAD_INV"
    RATIO_INV = "RATIO_INV"
    LOG = "LOG"
