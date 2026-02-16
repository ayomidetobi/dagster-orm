"""Schema definitions for metadata and value tables.

Contains table names, column names, enumerations, and data structures.
All table names and column names must be defined here to avoid magic strings.
"""

from datetime import datetime
from enum import Enum
from typing import List, TypedDict


class TableNames:
    """Table name constants."""

    METADATA = "metadata"
    VALUE = "value"


class MetadataColumns:
    """Metadata table column name constants."""

    SERIES_CODE = "series_code"
    ASSET_CLASS = "asset_class"
    SUB_ASSET_CLASS = "sub_asset_class"
    PRODUCT_TYPE = "product_type"
    DATA_TYPE = "data_type"
    MARKET_SEGMENT = "market_segment"
    COUNTRY = "country"
    CURRENCY = "currency"
    TICKER = "ticker"
    FIELD_TYPE = "field_type"
    REGION = "region"
    TERM = "term"
    TENOR = "tenor"
    STRUCTURE_TYPE = "structure_type"
    TICKER_SOURCE = "ticker_source"
    DATA_SOURCE = "data_source"
    SERIES_NAME = "series_name"
    VALID_FROM = "valid_from"
    VALID_TO = "valid_to"
    CALCULATION_FORMULA = "calculation_formula"
    DESCRIPTION = "description"


class ValueColumns:
    """Value table column name constants."""

    SERIES_CODE = "series_code"
    TIMESTAMP = "timestamp"
    VALUE = "value"


class TickerSource(str, Enum):
    """Ticker source enumeration."""

    BLOOMBERG = "Bloomberg"
    HAWKEYE = "Hawkeye"
    LSEG = "LSEG"
    RAMP = "Ramp"
    ONETICK = "OneTick"
    MANUAL_ENTRY = "Manual Entry"
    INTERNAL = "Internal"


class ControlTableType(str, Enum):
    """Control table type enumeration."""

    LOOKUP = "lookup"
    METADATA = "metadata"
    FIELD_MAP = "field_map"
    SERIES_DEPENDENCIES = "series_dependencies"


class TempTableName(str, Enum):
    """Temporary table name enumeration."""

    LOOKUP_TABLES = "_temp_lookup_tables"
    META_SERIES = "_temp_meta_series"
    SERIES_DEPENDENCIES = "_temp_series_dependencies"


class PreviewColumns:
    """Preview column names constants."""

    SERIES_CODE = "series_code"
    SERIES_NAME = "series_name"
    ASSET_CLASS = "asset_class"
    TICKER_SOURCE = "ticker_source"

    @classmethod
    def get_default_columns(cls) -> List[str]:
        """Get default preview columns list.

        Returns:
            List of default preview column names
        """
        return [
            cls.SERIES_CODE,
            cls.SERIES_NAME,
            cls.ASSET_CLASS,
            cls.TICKER_SOURCE,
        ]


class DataPoint(TypedDict):
    """Data point structure for time-series data.

    Represents a single data point with timestamp and value.
    Used consistently across PyPDL ingestion and value data operations.
    """

    timestamp: datetime
    value: float
