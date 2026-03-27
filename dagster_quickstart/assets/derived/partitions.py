"""Dagster static partitions for derived-series calculation types."""

from dagster import StaticPartitionsDefinition

# Hardcoded partition keys (keep aligned with ``CALCULATION_FORMULA_TYPES`` in orm.schema.constants).
DERIVED_CALC_PARTITIONS = StaticPartitionsDefinition(
    [
        "SPREAD",
        "FLY",
        "BOX",
        "RATIO",
        "SPREAD_INV",
        "RATIO_INV",
    ]
)

__all__ = ["DERIVED_CALC_PARTITIONS"]
