#!/usr/bin/env python3
"""Simple examples of how to use the rewrite DataAPI.

Zero-config: reads DATABASE_URL / S3_* from dagster_quickstart/.env (via
python-decouple) and attaches the real Postgres+S3 DuckLake catalog.
Assumes data/meta_series.csv has already been ingested (see
scripts/test_rewrite_data_api.py).

Usage:
    python scripts/example_dataapi.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


# Zero-config: wires the DuckLake connection, repositories, and vendor
# clients under the hood. live=True makes get_values()/get_last_values()
# default to fetching straight from the vendor instead of DuckLake.
data_api = DataAPI(live=False)

# Example 1: Query metadata with filters passed as keyword arguments
print_separator("Example 1: Query metadata with kwargs filters")
context = data_api.get_metadata(asset_class=["Equity", "Commodity", "Fixed Income"])
print(f"Found {len(context)} metadata rows")
print(f"series_codes: {context.series_codes[:5]}{'...' if len(context) > 5 else ''}")
if not context.empty:
    print(context.info.head())

# Example 2: Fetch values straight from that metadata result, no need to
# re-extract series_code yourself.
print_separator("Example 2: Fetch values directly from the metadata result")
values_df = context.get_values(ticker_source="mds")
print(f"Values shape: {values_df.shape}")
if not values_df.empty:
    print(values_df.tail(5))

data_api.write_values(frame = values_df)
last_values_df = context.get_last_values(ticker_source="BBG",tenor= "1y")
print("\nLatest value per series:")
if not last_values_df.empty:
    print(last_values_df.tail(1).T)

# Example 3: Discover what you can filter by
print_separator("Example 3: Discover available filter fields/options")
print(f"Metadata columns: {data_api.get_metadata_columns()}")
asset_class_options = data_api.filter_options(fields="asset_class")
print(f"asset_class options: {asset_class_options}")

# Narrow options to a subset, e.g. currencies used within Equity series
currency_within_equity = data_api.filter_options(
    fields="currency", filters={"asset_class": ["Equity"]}
)
print(f"currency options within asset_class=Equity: {currency_within_equity}")

# Example 4: strict controls how an unrecognized filter *value* is handled --
# not to be confused with an unrecognized filter *field*, which always raises.
print_separator("Example 4: strict vs lenient filter-value validation")
lenient = data_api.get_metadata(asset_class=["Equity", "NotARealAssetClass"])
print(f"strict=False (default): proceeds with the valid subset -> {len(lenient)} rows")

try:
    data_api.get_metadata(asset_class=["Equity", "NotARealAssetClass"], strict=True)
except InvalidFilterValueError as exc:
    print(f"strict=True: raised as expected -> {exc}")

# Example 5: groupby -- split a QuerySet into one QuerySet per group
print_separator("Example 5: groupby(asset_class)")
for (asset_class,), group in data_api.query().filter(
    asset_class=["Equity", "Commodity", "Fixed Income"]
).groupby("asset_class"):
    codes = group.metadata()[MetadataColumns.SERIES_CODE].tolist()
    print(f"{asset_class}: {len(codes)} series")

# Example 6: the fluent QuerySet builder -- filter/order/limit, chained
print_separator("Example 6: fluent QuerySet builder")
top_5_equity_values = (
    data_api.query()
    .filter(asset_class=["Equity"])
    .live("BBG")
    .limit(5)
    .value()
)
print(f"Top-5-row live Equity values:\n{top_5_equity_values}")

print_separator("All examples completed!")
