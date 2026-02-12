"""Constants for metadata and value tables.

Contains configuration constants like S3 paths, valid filter columns,
and processing order.
"""

from dagster_quickstart.orm.schema.schemas import MetadataColumns


VALID_METADATA_FILTER_COLUMNS = {
    MetadataColumns.SERIES_CODE,
    MetadataColumns.ASSET_CLASS,
    MetadataColumns.SUB_ASSET_CLASS,
    MetadataColumns.PRODUCT_TYPE,
    MetadataColumns.DATA_TYPE,
    MetadataColumns.MARKET_SEGMENT,
    MetadataColumns.COUNTRY,
    MetadataColumns.CURRENCY,
    MetadataColumns.TICKER,
    MetadataColumns.FIELD_TYPE,
    MetadataColumns.REGION,
    MetadataColumns.TERM,
    MetadataColumns.TENOR,
    MetadataColumns.STRUCTURE_TYPE,
    MetadataColumns.TICKER_SOURCE,
    MetadataColumns.DATA_SOURCE,
    MetadataColumns.SERIES_NAME,
}

LOOKUP_TABLE_PROCESSING_ORDER = [
    "asset_class",
    "product_type",
    "data_type",
    "structure_type",
    "market_segment",
    "field_type",
    "ticker_source",
    "sub_asset_class",
    "region",
    "currency",
    "term",
    "tenor",
]

S3_BASE_PATH_VALUE_DATA = "value-data"
S3_BASE_PATH_CONTROL = "control"
S3_PARQUET_FILE_NAME = "data.parquet"
