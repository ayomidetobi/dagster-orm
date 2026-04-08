"""Hawk (Hawkeye) ingestion assets."""

from dagster_quickstart.assets.ingestion import HAWK_FIELD_TYPE_PARTITIONS

from .asset import ingest_hawk_data_backfill, ingest_hawk_data_daily

__all__ = [
    "HAWK_FIELD_TYPE_PARTITIONS",
    "ingest_hawk_data_daily",
    "ingest_hawk_data_backfill",
]
