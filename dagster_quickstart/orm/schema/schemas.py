"""Schema definitions for metadata and value tables.

Contains table names, column names, enumerations, and data structures.
All table names and column names must be defined here to avoid magic strings.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, TypedDict, Union


class TableNames:
    """Table name constants."""

    METADATA = "metadata"
    METADATA_DERIVED = "metadata_derived"
    #: S3 segment ``metadata*`` → ``control/metadata*/data.parquet`` (primary + derived).
    METADATA_WILDCARD = "metadata*"
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
    # Not present on metadata Parquet; use BBG_FIELD / MDS_FIELD by vendor.
    FIELD_TYPE = "field_type"
    REGION = "region"
    TERM = "term"
    TENOR = "tenor"
    STRUCTURE_TYPE = "structure_type"
    # Not on metadata Parquet; vendor is implied by bbg_* vs mds_* columns.
    TICKER_SOURCE = "ticker_source"
    DEFAULT_SOURCE = "default_source"
    BBG_TICKER = "bbg_ticker"
    MDS_TICKER = "mds_ticker"
    BBG_FIELD = "bbg_field"
    BBG_DATA_TYPE = "bbg_data_type"
    MDS_FIELD = "mds_field"
    MDS_DATA_TYPE = "mds_data_type"
    HAWK_TICKER = "hawk_ticker"
    HAWK_FIELD = "hawk_field"
    DATA_SOURCE = "data_source"
    SERIES_NAME = "series_name"
    VALID_FROM = "valid_from"
    VALID_TO = "valid_to"
    CALCULATION_FORMULA = "calculation_formula"
    DES_NOTES = "des_notes"
    CALC_TYPE = "calc_type"
    PARENT_SERIES_CODE = "parent_series_code"
    DESCRIPTION = "description"

FilterValue = Union[str, List[str]]



class FilterParams(TypedDict, total=False):
    series_code: FilterValue
    asset_class: FilterValue
    sub_asset_class: FilterValue
    product_type: FilterValue
    data_type: FilterValue
    market_segment: FilterValue
    country: FilterValue
    currency: FilterValue
    ticker: FilterValue
    field_type: FilterValue
    region: FilterValue
    term: FilterValue
    tenor: FilterValue
    structure_type: FilterValue
    ticker_source: FilterValue
    default_source: FilterValue
    bbg_ticker: FilterValue
    mds_ticker: FilterValue
    bbg_field: FilterValue
    bbg_data_type: FilterValue
    mds_field: FilterValue
    mds_data_type: FilterValue
    hawk_ticker: FilterValue
    hawk_field: FilterValue
    data_source: FilterValue
    series_name: FilterValue
    valid_from: FilterValue
    valid_to: FilterValue
    calculation_formula: FilterValue
    des_notes: FilterValue
    calc_type: FilterValue
    description: FilterValue

    # extra (not strictly metadata column but used in logic)
    control_table: str


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
    MDS = "MDS"


@dataclass(frozen=True)
class TickerSourceConfig:
    """Centralized configuration for one ticker source."""

    ticker_column: Optional[str] = None
    field_column: Optional[str] = None
    wide_storage: bool = False


TICKER_SOURCE_REGISTRY: Dict[TickerSource, TickerSourceConfig] = {
    TickerSource.BLOOMBERG: TickerSourceConfig(
        ticker_column=MetadataColumns.BBG_TICKER,
        field_column=MetadataColumns.BBG_FIELD,
        wide_storage=True,
    ),
    TickerSource.HAWKEYE: TickerSourceConfig(
        ticker_column=MetadataColumns.HAWK_TICKER,
        field_column=MetadataColumns.HAWK_FIELD,
        wide_storage=True,
    ),
    TickerSource.LSEG: TickerSourceConfig(),
    TickerSource.RAMP: TickerSourceConfig(),
    TickerSource.ONETICK: TickerSourceConfig(
        ticker_column=MetadataColumns.MDS_TICKER,
        field_column=MetadataColumns.MDS_FIELD,
        wide_storage=False,
    ),
    TickerSource.MANUAL_ENTRY: TickerSourceConfig(),
    TickerSource.INTERNAL: TickerSourceConfig(
        field_column=MetadataColumns.CALC_TYPE,
        wide_storage=True,
    ),
    TickerSource.MDS: TickerSourceConfig(
        ticker_column=MetadataColumns.MDS_TICKER,
        field_column=MetadataColumns.MDS_FIELD,
        wide_storage=True,
    ),
}

VENDOR_TICKER_COLUMN_BY_SOURCE: Dict[TickerSource, str] = {
    source: config.ticker_column
    for source, config in TICKER_SOURCE_REGISTRY.items()
    if config.ticker_column is not None
}

VENDOR_FIELD_COLUMN_BY_SOURCE: Dict[TickerSource, str] = {
    source: config.field_column
    for source, config in TICKER_SOURCE_REGISTRY.items()
    if config.field_column is not None
    and source != TickerSource.INTERNAL
}


def get_ticker_source_config(ticker_source: TickerSource) -> TickerSourceConfig:
    """Return centralized config for a ticker source."""
    try:
        return TICKER_SOURCE_REGISTRY[ticker_source]
    except KeyError as exc:
        raise ValueError(f"Ticker source {ticker_source!r} is not registered") from exc


def ticker_source_uses_wide_storage(ticker_source: TickerSource) -> bool:
    """Return whether the ticker source reads from wide parquet storage."""
    return get_ticker_source_config(ticker_source).wide_storage


def get_storage_field_column(ticker_source: TickerSource) -> str:
    """Return metadata column used to resolve wide-storage field partitions."""
    field_column = get_ticker_source_config(ticker_source).field_column
    if field_column is None:
        raise ValueError(
            f"No storage field column for ticker_source={ticker_source!r}; "
            f"supported: {[source for source, cfg in TICKER_SOURCE_REGISTRY.items() if cfg.field_column]}"
        )
    return field_column


def get_vendor_ticker_column(ticker_source: TickerSource) -> str:
    """Return metadata ticker column for a vendor ticker source."""
    config = get_ticker_source_config(ticker_source)
    if config.ticker_column is None:
        raise ValueError(
            f"No ticker column for ticker_source={ticker_source!r}; "
            f"supported: {list(VENDOR_TICKER_COLUMN_BY_SOURCE.keys())}"
        )
    return config.ticker_column


def get_vendor_field_column(ticker_source: TickerSource) -> str:
    """Return metadata field column for a vendor ticker source."""
    config = get_ticker_source_config(ticker_source)
    if config.field_column is None or ticker_source == TickerSource.INTERNAL:
        raise ValueError(
            f"No field column for ticker_source={ticker_source!r}; "
            f"supported: {list(VENDOR_FIELD_COLUMN_BY_SOURCE.keys())}"
        )
    return config.field_column


def get_vendor_ticker_and_field_columns(ticker_source: TickerSource) -> Tuple[str, str]:
    """Return (ticker_column, field_column) metadata columns for ticker source."""
    return get_vendor_ticker_column(ticker_source), get_vendor_field_column(ticker_source)


class ControlTableType(str, Enum):
    """Control table type enumeration."""

    LOOKUP = "lookup"
    METADATA = "metadata"
    METADATA_DERIVED = "metadata_derived"
    FIELD_MAP = "field_map"


class TempTableName(str, Enum):
    """Temporary table name enumeration."""

    LOOKUP_TABLES = "_temp_lookup_tables"
    META_SERIES = "_temp_meta_series"
    METADATA_DERIVED = "_temp_metadata_derived"


class PreviewColumns:
    """Preview column names constants."""

    SERIES_CODE = "series_code"
    SERIES_NAME = "series_name"
    ASSET_CLASS = "asset_class"

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
        ]
