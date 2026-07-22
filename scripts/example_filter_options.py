#!/usr/bin/env python3
"""Example: discover metadata filter values with DataAPI.filter_options().

filter_options() answers "what can I actually filter get_metadata()/
get_values() by?" -- computed via SELECT DISTINCT against the catalog, not
by pulling the whole metadata table into pandas.

Assumes data/meta_series.csv has already been ingested (see
scripts/example_ingest_csv.py).

Usage:
    python scripts/example_filter_options.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

from rewrite.data_api.api.data_api import DataAPI
from rewrite.data_api.errors import InvalidFilterFieldError, InvalidFilterValueError


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


data_api = DataAPI()

# Example 1: What columns can I even filter by? (get_metadata_columns() is
# the field-name counterpart to filter_options()' field-value discovery.)
print_separator("Example 1: Discover filterable columns")
print(data_api.get_metadata_columns())

# Example 2: fields=None -- options for every column at once
print_separator("Example 2: filter_options() with no fields -- every column")
all_options = data_api.filter_options()
for field, values in all_options.items():
    print(f"{field}: {values[:5]}{' ...' if len(values) > 5 else ''}")

# Example 3: a single field -- returns a plain list
print_separator("Example 3: filter_options(fields='asset_class')")
print(data_api.filter_options(fields="asset_class"))

# Example 4: multiple fields -- returns a dict
print_separator("Example 4: filter_options(fields=['asset_class', 'region'])")
print(data_api.filter_options(fields=["asset_class", "region"]))

# Example 5: narrow options to a subset with `filters` -- e.g. currencies
# that actually appear within Equity series, not every currency in the catalog
print_separator("Example 5: narrow with filters= (currency within asset_class=Equity)")
print(data_api.filter_options(fields="currency", filters={"asset_class": ["Equity"]}))

# Example 6: exclude= -- currencies among everything that ISN'T Equity
print_separator("Example 6: filter_options(..., exclude=True)")
print(data_api.filter_options(fields="currency", filters={"asset_class": ["Equity"]}, exclude=True))

# Example 7: as_dataframe=True -- long-form field/value rows, handy for display
print_separator("Example 7: as_dataframe=True")
print(data_api.filter_options(fields=["asset_class", "region"], as_dataframe=True))

# Example 8: an unknown FIELD name always raises, regardless of strict
print_separator("Example 8: unknown field raises InvalidFilterFieldError")
try:
    data_api.filter_options(fields="not_a_real_column")
except InvalidFilterFieldError as exc:
    print(f"Raised as expected: {exc}")

# Example 9: strict controls how an unrecognized filter VALUE (not field) is
# handled, in the narrowing `filters` -- e.g. a typo'd asset_class
print_separator("Example 9: strict=False (default) vs strict=True on a bad value")
lenient = data_api.filter_options(fields="currency", filters={"asset_class": ["Equity", "NotARealAssetClass"]})
print(f"strict=False: proceeds with the valid subset -> {lenient}")

try:
    data_api.filter_options(
        fields="currency",
        filters={"asset_class": ["Equity", "NotARealAssetClass"]},
        strict=True,
    )
except InvalidFilterValueError as exc:
    print(f"strict=True: raised as expected -> {exc}")

print_separator("Done")
