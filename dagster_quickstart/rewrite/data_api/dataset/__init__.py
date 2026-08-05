"""Predefined semantic datasets built on top of DataAPI/QuerySet."""

from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase
from dagster_quickstart.rewrite.data_api.dataset.fx import FXMajor, FXSpot, FXUSDBloc

__all__ = [
    "DatasetBase",
    "FXMajor",
    "FXSpot",
    "FXUSDBloc",
]
