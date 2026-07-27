#!/usr/bin/env python3
"""Example: same series_code, different ticker_source, distinct saved data.

Fetches the same series live from three different vendors (Bloomberg, Hawk,
MDS -- each currently returns its own random demo values, see
rewrite/data_api/vendors/demo_data.py), saves each vendor's data into
DuckLake, then reads it back with live=False (i.e. straight from the S3
datalake, not a fresh vendor call) to prove each ticker_source's data was
saved to -- and is read back from -- its own partition.

Assumes data/meta_series.csv has already been ingested (see
scripts/example_ingest_csv.py) -- that's where SX0001_PX_LAST's
bbg_ticker/hawk_ticker/mds_ticker come from.

Usage:
    python scripts/example_same_series_multi_vendor.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

import pandas as pd

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

VENDORS = ["BBG", "HAWK", "MDS"]
SERIES_CODE = "SX0001_PX_LAST"


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


data_api = DataAPI()  # live=False by default -- we control out_of_cache per call below

print_separator(f"Fetching {SERIES_CODE} live from each vendor, and saving it")
for vendor in VENDORS:
    live_values = data_api.get_values([SERIES_CODE], ticker_source=vendor, out_of_cache=True)
    # write_values() tags every row with this vendor automatically -- it reads
    # the ticker_source get_values() left on live_values.attrs.
    data_api.write_values(live_values)
    print(f"{vendor}: fetched {len(live_values)} rows, saved to DuckLake")

print_separator("Reading back with live=False -- from the datalake, not the vendor")
per_vendor = {}
for vendor in VENDORS:
    cached_values = data_api.get_values([SERIES_CODE], ticker_source=vendor)
    per_vendor[vendor] = cached_values[SERIES_CODE]
    print(f"\n{vendor} (from S3, ticker_source-partitioned):")
    print(cached_values.tail(3))

print_separator("Side-by-side comparison")
comparison = pd.DataFrame(per_vendor)
print(comparison.tail(5))

print_separator("Confirming each vendor's saved data is genuinely distinct")
all_equal = (
    comparison["BBG"].equals(comparison["HAWK"])
    or comparison["BBG"].equals(comparison["MDS"])
    or comparison["HAWK"].equals(comparison["MDS"])
)
if all_equal:
    print("UNEXPECTED: two or more vendors returned identical saved data.")
else:
    print(
        f"Confirmed: {SERIES_CODE} has different saved values per ticker_source "
        f"({', '.join(VENDORS)}) -- each was written to and read back from its "
        f"own ticker_source partition in the datalake."
    )

print_separator("Done")
