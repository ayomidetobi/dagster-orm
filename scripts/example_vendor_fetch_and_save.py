#!/usr/bin/env python3
"""Example: fetch data live from different vendors and save it into DuckLake.

Zero-config: DataAPI() reads DATABASE_URL / S3_* from
dagster_quickstart/.env (via python-decouple) and attaches the real
Postgres+S3 DuckLake catalog under the hood. Assumes data/meta_series.csv
has already been ingested (see scripts/example_ingest_csv.py) -- that's
where each series' bbg_ticker/hawk_ticker/mds_ticker come from.

Every vendor client (Bloomberg/Hawk/MDS) currently returns random demo
values in place of a real SDK call, so this runs end-to-end with no vendor
credentials needed -- see rewrite/data_api/vendors/demo_data.py.

Usage:
    python scripts/example_vendor_fetch_and_save.py
"""

from locale import currency
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI
from dagster_quickstart.rewrite.data_api.columns import ValueColumns, normalize_ticker_source


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")



data_api = DataAPI(live=False)

end = datetime.now()
start = end - timedelta(days=30)

# Grab a handful of real series codes from the ingested metadata to fetch.
context = data_api.get_metadata()
series_codes = context.series_codes
print(f"Fetching {context.info['asset_class'].unique().tolist()} from each vendor")

# for vendor in ["BBG", "HAWK", "MDS"]:
#     print_separator(f"Fetching live from {vendor}")

#     # out_of_cache=True bypasses DuckLake and fetches straight from the
#     # vendor named by ticker_source.
#     wide_values = context.get_values(
#         ticker_source=vendor,
#         start=start,
#         end=end,
#         # out_of_cache=True,
#     )
#     print(f"Fetched {wide_values.shape[0]} rows x {wide_values.shape[1]} series live from {vendor}")
#     print(wide_values.tail(3))

#     data_api.write_values(wide_values)
#     print(f"Saved {len(wide_values)} rows to DuckLake, tagged ticker_source={normalize_ticker_source(vendor)!r}")

# # Read one of them back from the cache (DuckLake), not live from the vendor.
# print_separator("Reading saved BBG values back from DuckLake")
# cached_values = context.get_values(ticker_source="BBG")
# print(cached_values.tail(3))

# print_separator("Done")
