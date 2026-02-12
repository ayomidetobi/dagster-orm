"""Dagster resources for the financial platform."""

from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.resources.outlook_email_resource import OutlookEmailResource
from dagster_quickstart.resources.pypdl_resource import PyPDLResource

__all__ = [
    "DuckDBResource",
    "OutlookEmailResource",
    "PyPDLResource",
]
