"""Derived-series assets.

This package contains assets and helpers for calculating derived time-series
from existing parent series using DuckDB.
"""

from dagster_quickstart.assets.derived.asset import calculate_derived_series
from dagster_quickstart.assets.derived.partitions import DERIVED_CALC_PARTITIONS

__all__ = ["calculate_derived_series", "DERIVED_CALC_PARTITIONS"]
