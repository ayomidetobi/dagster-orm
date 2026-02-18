#!/usr/bin/env python3
"""Simple script template to read metadata info and value data using DataAPI.

Usage:
    python scripts/test_dataapi.py
    # Or with environment variables:
    S3_BUCKET=my-bucket S3_ACCESS_KEY=xxx S3_SECRET_KEY=xxx python scripts/test_dataapi.py
"""

import sys
from pathlib import Path

# Add parent directory to path to import dagster_quickstart
sys.path.insert(0, str(Path(__file__).parent.parent))

from decouple import config

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import TickerSource
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    """Print a separator line with optional text.

    Args:
        text: Optional text to print (default: empty string)
        char: Character to use for separator (default: "=")
        length: Length of separator line (default: 60)
    """
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


# Initialize DuckDB resource
duckdb_cacher = duckdb_datacacher(
    bucket=config("S3_BUCKET", default=None),
    access_key=config("S3_ACCESS_KEY", default=None),
    secret_key=config("S3_SECRET_KEY", default=None),
    region=config("S3_REGION", default=None),
)

duckdb_resource = DuckDBResource(cacher=duckdb_cacher)
duckdb_resource.setup_for_execution(None)

# Create DataAPI instance
data_api = DataAPI(duckdb_resource)

# # Example 1: Query metadata with filters
# print_separator("Example 1: Query metadata")
# dataset = data_api.get(
#     asset_class=["Commodity", "Equity"],
# )

# metadata_df = dataset.info()
# print(f"Found {len(metadata_df)} metadata rows")
# print(f"Columns: {', '.join(metadata_df.columns)}")
# if not metadata_df.empty:
#     print(f"\nFirst row:\n{metadata_df}")

# # Example 2: Get value data for the filtered series
# print_separator("Example 2: Get value data")
# values_df = dataset.value(
#     ValueQueryParams(
#         start="2025-02-01",
#         end="2026-02-16",
#     )
# )
# print(values_df.head(10))
# if not values_df.empty:
#     print(f"Columns: {', '.join(values_df.columns)}")
#     print(f"Date range: {values_df['timestamp'].min()} to {values_df['timestamp'].max()}")

# # Example 3: Test get_excluding - exclude certain regions
# print_separator("Example 3: Test get_excluding (exclude region='North America')")
# exclude_dataset = data_api.get_excluding(region="North America")
# exclude_metadata_df = exclude_dataset.info()
# print(f"Found {len(exclude_metadata_df)} metadata rows (excluding region='North America')")
# if not exclude_metadata_df.empty:
#     print(f"Regions in result: {exclude_metadata_df['region'].unique().tolist()}")
#     print(f"\nFirst few rows:\n{exclude_metadata_df[['series_code', 'region', 'asset_class']].head()}")

# # Example 4: Test get_last_values - get latest value for specific series
# print_separator("Example 4: Test get_last_values")
# # First, get some series codes to test with
# test_series_codes = data_api.get_series_codes(asset_class=["Equity"])
# if test_series_codes:
#     # Use first 3 series codes for testing
#     test_codes = test_series_codes[:3]
#     print(f"Testing with series codes: {test_codes}")
#     last_values_df = data_api.get_last_values(
#         series_codes=test_codes,
#         ticker_source=TickerSource.BLOOMBERG
#     )
#     print(f"Found {len(last_values_df)} last values")
#     if not last_values_df.empty:
#         print(f"\nLast values:\n{last_values_df}")
#     else:
#         print("No last values found (may need value data to be loaded first)")
# else:
#     print("No series codes found to test with")

# # Example 5: Test get_values - get all values for a ticker source
# print_separator("Example 5: Test get_values (all values for Bloomberg ticker source)")
# all_values_df = data_api.get_values(ticker_source=TickerSource.BLOOMBERG)
# print(f"Found {len(all_values_df)} total value rows for Bloomberg")
# if not all_values_df.empty:
#     print(f"Columns: {', '.join(all_values_df.columns)}")
#     print(f"Unique series codes: {all_values_df['series_code'].nunique()}")
#     print(f"Date range: {all_values_df['timestamp'].min()} to {all_values_df['timestamp'].max()}")
#     print(f"\nFirst 10 rows:\n{all_values_df.head(10)}")
# else:
#     print("No values found for Bloomberg ticker source")

# # Example 6: Compare get() vs get_excluding()
# print_separator("Example 6: Compare get() vs get_excluding()")
# # Get all Equity series
# all_equity = data_api.get(asset_class=["Equity"])
# all_equity_df = all_equity.info()
# print(f"get(asset_class=['Equity']): {len(all_equity_df)} rows")

# # Exclude North America region
# exclude_na = data_api.get_excluding(region="North America")
# exclude_na_df = exclude_na.info()
# print(f"get_excluding(region='North America'): {len(exclude_na_df)} rows")

# if not all_equity_df.empty and not exclude_na_df.empty:
#     all_regions = set(all_equity_df['region'].dropna().unique())
#     exclude_regions = set(exclude_na_df['region'].dropna().unique())
#     print(f"Regions in get(): {sorted(all_regions)}")
#     print(f"Regions in get_excluding(region='North America'): {sorted(exclude_regions)}")
#     print(f"North America excluded: {'North America' not in exclude_regions}")

# # Example 7: Test union() - unite 3 queries
# print_separator("Example 7: Test union() - unite 3 queries")
# # Create three different QuerySets
# qs1 = data_api.get(asset_class=["Equity"])
# qs2 = data_api.get(asset_class=["Commodity"])
# qs3 = data_api.get(asset_class=["Fixed Income"])

# # Get info for each to see what we're working with
# qs1_info = qs1.info()
# qs2_info = qs2.info()
# qs3_info = qs3.info()

# print(f"QuerySet 1 (Equity): {len(qs1_info)} series")
# print(f"QuerySet 2 (Commodity): {len(qs2_info)} series")
# print(f"QuerySet 3 (Fixed Income): {len(qs3_info)} series")

# # Get series codes for each
# qs1_codes = set(qs1_info['series_code'].unique())
# qs2_codes = set(qs2_info['series_code'].unique())
# qs3_codes = set(qs3_info['series_code'].unique())

# print(f"\nQuerySet 1 series codes: {sorted(qs1_codes)}")
# print(f"QuerySet 2 series codes: {sorted(qs2_codes)}")
# print(f"QuerySet 3 series codes: {sorted(qs3_codes)}")

# # Union all three QuerySets
# print("\nUniting QuerySet 1 and QuerySet 2...")
# qs_union_12 = qs1.union(qs2)
# qs_union_12_info = qs_union_12.info()
# qs_union_12_codes = set(qs_union_12_info['series_code'].unique())

# print(f"Union of QS1 and QS2: {len(qs_union_12_info)} series")
# print(f"Union series codes: {sorted(qs_union_12_codes)}")
# print(f"Expected union size: {len(qs1_codes | qs2_codes)}")
# print(f"Union correct: {qs_union_12_codes == (qs1_codes | qs2_codes)}")

# # Now union with the third QuerySet
# print("\nUniting (QS1 ∪ QS2) with QS3...")
# qs_union_all = qs_union_12.union(qs3)
# qs_union_all_info = qs_union_all.info()
# qs_union_all_codes = set(qs_union_all_info['series_code'].unique())

# print(f"Final union (all 3): {len(qs_union_all_info)} series")
# print(f"Final union series codes: {sorted(qs_union_all_codes)}")
# print(f"Expected union size: {len(qs1_codes | qs2_codes | qs3_codes)}")
# print(f"Union correct: {qs_union_all_codes == (qs1_codes | qs2_codes | qs3_codes)}")

# # Verify original QuerySets are unchanged
# print("\nVerifying original QuerySets are unchanged...")
# qs1_after = qs1.info()
# qs2_after = qs2.info()
# qs3_after = qs3.info()
# print(f"QS1 unchanged: {len(qs1_after) == len(qs1_info)}")
# print(f"QS2 unchanged: {len(qs2_after) == len(qs2_info)}")
# print(f"QS3 unchanged: {len(qs3_after) == len(qs3_info)}")

# # Test that we can get values from the unioned QuerySet
# print("\nTesting value() on unioned QuerySet...")
# try:
#     union_values = qs_union_all.value(
#         ValueQueryParams(
#             start="2025-01-01",
#             end="2025-12-31",
#         )
#     )
#     print(f"Successfully retrieved {len(union_values)} value rows from unioned QuerySet")
#     if not union_values.empty:
#         print(f"Unique series codes in values: {union_values['series_code'].nunique()}")
#         print(f"Series codes: {sorted(union_values['series_code'].unique())}")
# except Exception as e:
#     print(f"Note: Could not retrieve values (may need data loaded): {e}")

# Example 8: Test chained filter() - Dataset → Subset → Smaller subset
print_separator("Example 8: Test chained filter() - Dataset → Subset → Smaller subset")
# Start with a dataset
dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
dataset_info = dataset.info()
print(f"Initial dataset: {len(dataset_info)} series")
print(f"Asset classes: {sorted(dataset_info['asset_class'].unique())}")

# Filter to a subset
subset = dataset.filter(asset_class=["Equity", "Commodity"])
subset_info = subset.info()
print(f"\nAfter filter(asset_class=['Equity', 'Commodity']): {len(subset_info)} series")
print(f"Asset classes: {sorted(subset_info['asset_class'].unique())}")

# Filter to a smaller subset
smaller_subset = subset.filter(region="North America")
smaller_subset_info = smaller_subset.info()
print(f"\nAfter filter(region='North America'): {len(smaller_subset_info)} series")
print(f"Regions: {sorted(smaller_subset_info['region'].unique())}")
print(f"Asset classes: {sorted(smaller_subset_info['asset_class'].unique())}")

# Verify original dataset is unchanged
print(f"\nOriginal dataset unchanged: {len(dataset.info()) == len(dataset_info)}")
print(f"Subset unchanged: {len(subset.info()) == len(subset_info)}")

# Test that we can get values from the filtered QuerySet
print("\nTesting value() on filtered QuerySet...")
try:
    filtered_values = smaller_subset.value(
        ValueQueryParams(
            start="2025-01-01",
            end="2025-12-31",
        )
    )
    print(f"Successfully retrieved {len(filtered_values)} value rows from filtered QuerySet")
    if not filtered_values.empty:
        print(f"Unique series codes: {filtered_values['series_code'].nunique()}")
except Exception as e:
    print(f"Note: Could not retrieve values (may need data loaded): {e}")

# Example 9: Test chained filter_exclude() - Dataset → Exclude → Exclude more
print_separator("Example 9: Test chained filter_exclude() - Dataset → Exclude → Exclude more")
# Start with a dataset
dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
dataset_info = dataset.info()
print(f"Initial dataset: {len(dataset_info)} series")
print(f"Asset classes: {sorted(dataset_info['asset_class'].unique())}")
print(f"Regions: {sorted(dataset_info['region'].dropna().unique())}")

# Exclude some asset classes
excluded_subset = dataset.filter_exclude(asset_class=["Fixed Income"])
excluded_subset_info = excluded_subset.info()
print(f"\nAfter filter_exclude(asset_class=['Fixed Income']): {len(excluded_subset_info)} series")
print(f"Asset classes: {sorted(excluded_subset_info['asset_class'].unique())}")

# Exclude more (regions)
smaller_excluded = excluded_subset.filter_exclude(region="North America")
smaller_excluded_info = smaller_excluded.info()
print(f"\nAfter filter_exclude(region='North America'): {len(smaller_excluded_info)} series")
print(f"Regions: {sorted(smaller_excluded_info['region'].dropna().unique())}")
print(f"Asset classes: {sorted(smaller_excluded_info['asset_class'].unique())}")

# Verify original dataset is unchanged
print(f"\nOriginal dataset unchanged: {len(dataset.info()) == len(dataset_info)}")
print(f"Excluded subset unchanged: {len(excluded_subset.info()) == len(excluded_subset_info)}")

# Test that we can get values from the filtered QuerySet
print("\nTesting value() on filtered QuerySet...")
try:
    filtered_values = smaller_excluded.value(
        ValueQueryParams(
            start="2025-01-01",
            end="2025-12-31",
        )
    )
    print(f"Successfully retrieved {len(filtered_values)} value rows from filtered QuerySet")
    if not filtered_values.empty:
        print(f"Unique series codes: {filtered_values['series_code'].nunique()}")
except Exception as e:
    print(f"Note: Could not retrieve values (may need data loaded): {e}")

# Example 10: Test mixing filter() and filter_exclude()
print_separator("Example 10: Test mixing filter() and filter_exclude()")
mixed_dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
print(f"Initial: {len(mixed_dataset.info())} series")

# First filter (include)
filtered = mixed_dataset.filter(asset_class=["Equity", "Commodity"])
print(f"After filter(asset_class=['Equity', 'Commodity']): {len(filtered.info())} series")

# Then exclude
excluded = filtered.filter_exclude(region="North America")
excluded_info = excluded.info()
print(f"After filter_exclude(region='North America'): {len(excluded_info)} series")
print(f"Regions: {sorted(excluded_info['region'].dropna().unique())}")
print(f"Asset classes: {sorted(excluded_info['asset_class'].unique())}")

print_separator("All tests completed!")
