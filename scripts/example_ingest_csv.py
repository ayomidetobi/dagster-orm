#!/usr/bin/env python3
"""Example: ingest a metadata CSV/Excel file from data/ into DuckLake (S3-backed).

Zero-config: DataAPI(live=True) reads DATABASE_URL / S3_* from
dagster_quickstart/.env (via python-decouple) and attaches the real
Postgres+S3 DuckLake catalog under the hood. import_metadata(path=...)
writes the validated rows straight into the DuckLake metadata table --
physically Parquet files under s3://<bucket>/ducklake/, tracked by the
Postgres catalog and versioned by DuckLake's own snapshot-per-write model.

Usage:
    python scripts/example_ingest_csv.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

from rewrite.data_api.api.data_api import DataAPI


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


data_api = DataAPI(live=True)

# Example 1: Ingest a CSV file straight from disk -- one call, no manual
# pd.read_csv()/pd.read_excel() needed. import_metadata() returns the
# validated rows that were written.
print_separator("Example 1: Ingest a CSV file")
csv_path = DAGSTER_QUICKSTART / "data" / "meta_series.csv"
ingested = data_api.import_metadata(path=csv_path)
print(f"Ingested {len(ingested)} rows from {csv_path.name}")

# Confirm the round trip by querying it back with get_metadata().
context = data_api.get_metadata(series_code=ingested["series_code"].head(3).tolist())
print(f"\nQueried back {len(context)} of those rows:")
print(context.frame.head(3))

# Example 2: Ingest an Excel file, selecting a sheet by name (or pass an
# int for a positional index). Same signature, same return shape.
print_separator("Example 2: Ingest an Excel file with a specific sheet")
xlsx_path = DAGSTER_QUICKSTART / "data" / "meta_series.xlsx"
if xlsx_path.exists():
    ingested_xlsx = data_api.import_metadata(path=xlsx_path, sheet="metadata")
    print(f"Ingested {len(ingested_xlsx)} rows from {xlsx_path.name} (sheet='metadata')")
else:
    print(f"(skipped -- {xlsx_path.name} not present in data/; same call works for any .xlsx/.xls)")

# Example 3: Ingest an in-memory DataFrame directly -- for data built in
# code rather than read from a file.
print_separator("Example 3: Ingest an in-memory DataFrame")
import pandas as pd

extra_row = pd.DataFrame(
    [{"series_code": "EXAMPLE_SERIES_001", "series_name": "Example series", "asset_class": "Equity"}]
)
ingested_frame = data_api.import_metadata(frame=extra_row)
print(f"Ingested {len(ingested_frame)} row(s) built in code:")
print(ingested_frame)

print_separator("Done")
