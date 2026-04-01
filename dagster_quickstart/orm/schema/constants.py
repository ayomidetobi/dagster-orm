"""Constants for metadata and value tables.

Contains configuration constants like S3 paths, valid filter columns,
and processing order.
"""

from typing import Dict

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
    MetadataColumns.REGION,
    MetadataColumns.TERM,
    MetadataColumns.TENOR,
    MetadataColumns.STRUCTURE_TYPE,
    MetadataColumns.DATA_SOURCE,
    MetadataColumns.SERIES_NAME,
    MetadataColumns.BBG_TICKER,
    MetadataColumns.MDS_TICKER,
    MetadataColumns.BBG_FIELD,
    MetadataColumns.MDS_FIELD,
    MetadataColumns.BBG_DATA_TYPE,
    MetadataColumns.MDS_DATA_TYPE,
    # Dependency rows (``metadata_derived``) and ``field_type`` → ``calc_type`` filters.
    MetadataColumns.CALC_TYPE,
}

COLUMN_GROUPS = {
        "IDENTIFIERS": [MetadataColumns.SERIES_NAME, MetadataColumns.SERIES_CODE],
        "CLASSIFICATION": [MetadataColumns.ASSET_CLASS, MetadataColumns.SUB_ASSET_CLASS, MetadataColumns.PRODUCT_TYPE, MetadataColumns.STRUCTURE_TYPE],
        "MARKET": [MetadataColumns.MARKET_SEGMENT, MetadataColumns.REGION, MetadataColumns.CURRENCY],
        "TERM": [MetadataColumns.TERM, MetadataColumns.TENOR],
        "BLOOMBERG": [MetadataColumns.BBG_FIELD, MetadataColumns.BBG_DATA_TYPE, MetadataColumns.BBG_TICKER],
        "MDS": [MetadataColumns.MDS_FIELD, MetadataColumns.MDS_DATA_TYPE, MetadataColumns.MDS_TICKER],
        "VALIDITY": [MetadataColumns.VALID_FROM, MetadataColumns.VALID_TO],
        "OTHER": [MetadataColumns.CALCULATION_FORMULA, MetadataColumns.DES_NOTES],
    }

LOOKUP_TABLE_PROCESSING_ORDER = [
    "asset_class",
    "product_type",
    "structure_type",
    "market_segment",
    "sub_asset_class",
    "region",
    "currency",
    "term",
    "tenor",
]

S3_BASE_PATH_VALUE_DATA = "value-data"
S3_BASE_PATH_CONTROL = "control"
S3_PARQUET_FILE_NAME = "data.parquet"

# Wide Parquet partition key for internally computed (derived) series when metadata has no vendor field.
INTERNAL_WIDE_PARTITION_FIELD = "DERIVED"

# Dagster run metadata: cap invalid_details rows / string length (e.g. Bloomberg ingestion checks).
MAX_INVALID_METADATA_ROWS = 20
MAX_INVALID_VALUE_CHARS = 500

CALCULATION_FORMULA_TYPES: Dict[str, int] = {
    "SPREAD": 2,  # parent[0] - parent[1]
    "FLY": 3,
    "BOX": 4,
    "RATIO": 2,  # parent[0] / parent[1]
    "SPREAD_INV": 2,  # parent[1] - parent[0]
    "RATIO_INV": 2,  # parent[1] / parent[0]
}
