"""Derived-series assets.

This package contains assets and helpers for calculating derived time-series
from existing parent series using DuckDB.
"""

from dagster_quickstart.assets.derived.asset import calculate_derived_series

__all__ = ["calculate_derived_series"]
