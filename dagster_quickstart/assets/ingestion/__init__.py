"""Ingestion assets package and shared partition definitions."""

from dagster import StaticPartitionsDefinition

FIELD_TYPE_PARTITIONS = StaticPartitionsDefinition(
    [
        "PX_LAST",
        "PX_OPEN",
        "PX_HIGH",
        "PX_LOW",
        "PX_VOLUME",
        "YIELD_CURVE",
        "SPREAD",
        "RATE",
    ]
)

HAWK_FIELD_TYPE_PARTITIONS = StaticPartitionsDefinition(["vol"])

__all__ = ["FIELD_TYPE_PARTITIONS", "HAWK_FIELD_TYPE_PARTITIONS"]
