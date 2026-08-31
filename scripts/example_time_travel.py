#!/usr/bin/env python3
"""Example: DuckLake time travel -- query the data lake as it was in the past.

DuckLake never overwrites or deletes on a normal write -- every
write_values()/import_metadata() call creates a new snapshot on top of the
last one. That means a "corrected" value doesn't erase the old one; it just
adds a newer row on top of it. Passing as_of=<timestamp> (or version=<n>) to
get_values()/get_metadata() re-runs the query against the data lake exactly
as it looked at that point in time, instead of the latest state.

Usage:
    python scripts/example_time_travel.py
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

import pandas as pd

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

SERIES_CODE = "TIME_TRAVEL_DEMO"


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


data_api = DataAPI()

print_separator("Step 1: write an initial value")
data_api.write_values(
    pd.DataFrame(
        {
            "series_code": [SERIES_CODE],
            "timestamp": pd.to_datetime(["2024-01-01"]),
            "value": [100.0],
        }
    )
)
as_of_before_correction = datetime.now(timezone.utc)
print(f"Wrote value=100.0. Marked as_of={as_of_before_correction.isoformat()}")

# Make sure the correction below lands with a strictly later timestamp.
time.sleep(1.5)

print_separator("Step 2: write a correction for the SAME series/timestamp")
data_api.write_values(
    pd.DataFrame(
        {
            "series_code": [SERIES_CODE],
            "timestamp": pd.to_datetime(["2024-01-01"]),
            "value": [200.0],
        }
    )
)
print("Wrote value=200.0 -- DuckLake keeps both rows; nothing was deleted.")

print_separator("Step 3: the latest query sees the correction")
latest = data_api.get_values([SERIES_CODE])
print(latest)

print_separator("Step 4: as_of= time travel sees the value as it was before the correction")
historical = data_api.get_values([SERIES_CODE], as_of=as_of_before_correction)
print(historical)

print_separator("Confirming")
if latest[SERIES_CODE].iloc[-1] == 200.0 and historical[SERIES_CODE].iloc[-1] == 100.0:
    print("Confirmed: latest query = 200.0 (corrected), as_of query = 100.0 (historical).")
else:
    print("UNEXPECTED: time travel did not return the expected historical value.")

print_separator("Step 5: as_of takes a full timestamp, not just a date")
# as_of is a plain datetime -- hour/minute/second/microsecond all matter, not
# just the date. A hand-typed moment works exactly like datetime.now() did
# above: build it with whatever precision you actually mean. Note that
# datetime(2020, 1, 1) with no time-of-day means midnight (00:00:00) that
# day, not "sometime on 2020-01-01" -- pass the hour/minute (as below)
# whenever the distinction matters, e.g. picking a point between two
# same-day snapshots.
precise_as_of = datetime(2020, 1, 1, 15, 45, tzinfo=timezone.utc)
print(f"Querying as_of={precise_as_of.isoformat()} (2020-01-01, 15:45 -- long before this demo ran)")
try:
    data_api.get_values([SERIES_CODE], as_of=precise_as_of)
except Exception as exc:
    print(
        "DuckLake raised (rather than returning an empty frame) because no "
        f"snapshot exists yet that far back: {exc}"
    )

print_separator("Done")
