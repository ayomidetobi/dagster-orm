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
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.orm.schema import TickerSource

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

# Create DataAPI instance (will use environment variables by default)
data_api = DataAPI()

# Example 1: Query metadata with filters
print_separator("Example 1: Query metadata")
dataset = data_api.get()

metadata_df = dataset.info()
print(dataset)
print(f"Found {len(metadata_df)} metadata rows")
# print(f"Columns: {', '.join(metadata_df.columns)}")
if not metadata_df.empty:
    print(f"\nFirst row:\n{metadata_df}")

# Example 2: Test global and contextual filter options
print_separator("Example 2: Test filter options")

global_asset_class_options = data_api.filter_options(fields="asset_class")
print(f"Global asset_class options: {global_asset_class_options}")

context_dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
print(context_dataset)

# context_currency_options = context_dataset.filter_options()
# print(f"Context currency options: {context_currency_options}")

context_option_table = context_dataset.filter_options(
    ["region", "currency"],
    as_dataframe=True,
).T
print("\nContext options as DataFrame:")
print(context_option_table)

# Example 3: Test repr() for chained include/exclude filters
# print_separator("Example 3: Test QuerySet repr")

# repr_dataset = (
#     data_api.get(asset_class="Equity")
#     .filter(region="North America")
#     .filter_exclude(currency="USD")
# )
# print(repr_dataset)

# Example 4: Get value data for the filtered series
print_separator("Example 2: Get value data")
values_df = dataset.get_last_values(ticker_source=TickerSource.BLOOMBERG).T
print(values_df.head(10))
# if not values_df.empty:
#     print(f"Columns: {', '.join(values_df.columns)}")
# for group, qs in dataset.groupby(["sub_asset_class", "region"]):
#     print( f"Group: {group}")
#     print( f"QuerySet: {qs}")
#     data = qs.get_values()
#     print(data.head(10))

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
#     last_values_df = exclude_dataset.get_last_values(
#         # series_codes=test_codes,
#         # ticker_source=TickerSource.BLOOMBERG
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
# all_values_df = exclude_dataset.get_values()
# print(f"Found {len(all_values_df)} total value rows for Bloomberg")
# if not all_values_df.empty:
#     # print(f"Columns: {', '.join(all_values_df.columns)}")
#     # print(f"Unique series codes: {all_values_df['series_code'].nunique()}")
#     # print(f"Date range: {all_values_df['timestamp'].min()} to {all_values_df['timestamp'].max()}")
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
# # exclude_na = data_api.get_excluding(region="North America")
# # exclude_na_df = exclude_na.info()
# # print(f"get_excluding(region='North America'): {len(exclude_na_df)} rows")

# # if not all_equity_df.empty and not exclude_na_df.empty:
# #     all_regions = set(all_equity_df['region'].dropna().unique())
# #     exclude_regions = set(exclude_na_df['region'].dropna().unique())
# #     print(f"Regions in get(): {sorted(all_regions)}")
# #     print(f"Regions in get_excluding(region='North America'): {sorted(exclude_regions)}")
# #     print(f"North America excluded: {'North America' not in exclude_regions}")

# # Example 7: Test union() - unite multiple queries
# print_separator("Example 7: Test union() - unite multiple queries")
# # Create three different QuerySets
# data_equity = data_api.get(asset_class=["Equity"])
# data_commodity = data_api.get(asset_class=["Commodity"])
# data_fixed_income = data_api.get(asset_class=["Fixed Income"])

# # Get info for each to see what we're working with
# equity_info = data_equity.info()
# commodity_info = data_commodity.info()
# fixed_income_info = data_fixed_income.info()

# print(f"Equity dataset: {len(equity_info)} series")
# print(f"Commodity dataset: {len(commodity_info)} series")
# print(f"Fixed Income dataset: {len(fixed_income_info)} series")

# # Get series codes for each
# equity_codes = set(equity_info["series_code"].unique())
# commodity_codes = set(commodity_info["series_code"].unique())
# fixed_income_codes = set(fixed_income_info["series_code"].unique())

# print(f"\nEquity series codes: {sorted(equity_codes)}")
# print(f"Commodity series codes: {sorted(commodity_codes)}")
# print(f"Fixed Income series codes: {sorted(fixed_income_codes)}")

# # Test union with two QuerySets
# print("\nUniting Equity and Commodity datasets...")
# combined_equity_commodity = data_equity.union(data_commodity)
# combined_info = combined_equity_commodity.info()
# combined_codes = set(combined_info["series_code"].unique())

# print(f"Union of Equity and Commodity: {len(combined_info)} series")
# print(f"Union series codes: {sorted(combined_codes)}")
# print(f"Expected union size: {len(equity_codes | commodity_codes)}")
# print(f"Union correct: {combined_codes == (equity_codes | commodity_codes)}")

# # Test union with multiple QuerySets at once
# print("\nUniting all three datasets at once...")
# combined_all = data_equity.union(data_commodity, data_fixed_income)
# combined_all_info = combined_all.info()
# combined_all_codes = set(combined_all_info["series_code"].unique())

# print(f"Final union (all 3): {len(combined_all_info)} series")
# print(f"Final union series codes: {sorted(combined_all_codes)}")
# print(f"Expected union size: {len(equity_codes | commodity_codes | fixed_income_codes)}")
# print(
#     f"Union correct: {combined_all_codes == (equity_codes | commodity_codes | fixed_income_codes)}"
# )

# # Verify original QuerySets are unchanged
# print("\nVerifying original QuerySets are unchanged...")
# equity_after = data_equity.info()
# commodity_after = data_commodity.info()
# fixed_income_after = data_fixed_income.info()
# print(f"Equity unchanged: {len(equity_after) == len(equity_info)}")
# print(f"Commodity unchanged: {len(commodity_after) == len(commodity_info)}")
# print(f"Fixed Income unchanged: {len(fixed_income_after) == len(fixed_income_info)}")

# # Test that we can get values from the unioned QuerySet
# print("\nTesting value() on unioned QuerySet...")
# try:
#     union_values = combined_all.value(
#         ValueQueryParams(
#             start="2025-01-01",
#             end="2025-12-31",
#         )
#     )
#     print("Successfully retrieved value data from unioned QuerySet")
#     print(f"Shape: {union_values.shape}")
#     if not union_values.empty:
#         print(f"Columns: {list(union_values.columns)}")
#         print(f"\nFirst few rows:\n{union_values.head()}")
# except Exception as e:
#     print(f"Note: Could not retrieve values (may need data loaded): {e}")

# # Example 8: Test chained filter() - Dataset → Subset → Smaller subset
# print_separator("Example 8: Test chained filter() - Dataset → Subset → Smaller subset")
# # Start with a dataset
# dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
# dataset_info = dataset.info()
# print(f"Initial dataset: {len(dataset_info)} series")
# print(f"Asset classes: {sorted(dataset_info['asset_class'].unique())}")

# # Filter to a subset
# subset = dataset.filter(asset_class=["Equity", "Commodity"])
# subset_info = subset.info()
# print(f"\nAfter filter(asset_class=['Equity', 'Commodity']): {len(subset_info)} series")
# print(f"Asset classes: {sorted(subset_info['asset_class'].unique())}")

# # Filter to a smaller subset
# smaller_subset = subset.filter(region="North America")
# smaller_subset_info = smaller_subset.info()
# print(f"\nAfter filter(region='North America'): {len(smaller_subset_info)} series")
# print(f"Regions: {sorted(smaller_subset_info['region'].unique())}")
# print(f"Asset classes: {sorted(smaller_subset_info['asset_class'].unique())}")

# # Verify original dataset is unchanged
# print(f"\nOriginal dataset unchanged: {len(dataset.info()) == len(dataset_info)}")
# print(f"Subset unchanged: {len(subset.info()) == len(subset_info)}")

# # Test that we can get values from the filtered QuerySet
# print("\nTesting value() on filtered QuerySet...")
# try:
#     filtered_values = smaller_subset.value(
#         ValueQueryParams(
#             start="2025-01-01",
#             end="2025-12-31",
#         )
#     )
#     print(filtered_values.head(10))
#     print(f"Successfully retrieved {len(filtered_values)} value rows from filtered QuerySet")
#     if not filtered_values.empty:
#         print(f"Unique series codes: {filtered_values['series_code'].nunique()}")
#         print(filtered_values.head(10))
# except Exception as e:
#     print(f"Note: Could not retrieve values (may need data loaded): {e}")

# # Example 9: Test chained filter_exclude() - Dataset → Exclude → Exclude more
# print_separator("Example 9: Test chained filter_exclude() - Dataset → Exclude → Exclude more")
# # Start with a dataset
# dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
# dataset_info = dataset.info()
# print(f"Initial dataset: {len(dataset_info)} series")
# print(f"Asset classes: {sorted(dataset_info['asset_class'].unique())}")
# print(f"Regions: {sorted(dataset_info['region'].dropna().unique())}")

# # Exclude some asset classes
# excluded_subset = dataset.filter_exclude(asset_class=["Fixed Income"])
# excluded_subset_info = excluded_subset.info()
# print(f"\nAfter filter_exclude(asset_class=['Fixed Income']): {len(excluded_subset_info)} series")
# print(f"Asset classes: {sorted(excluded_subset_info['asset_class'].unique())}")

# # Exclude more (regions)
# smaller_excluded = excluded_subset.filter_exclude(region="North America")
# smaller_excluded_info = smaller_excluded.info()
# print(f"\nAfter filter_exclude(region='North America'): {len(smaller_excluded_info)} series")
# print(f"Regions: {sorted(smaller_excluded_info['region'].dropna().unique())}")
# print(f"Asset classes: {sorted(smaller_excluded_info['asset_class'].unique())}")

# # Verify original dataset is unchanged
# print(f"\nOriginal dataset unchanged: {len(dataset.info()) == len(dataset_info)}")
# print(f"Excluded subset unchanged: {len(excluded_subset.info()) == len(excluded_subset_info)}")

# # Test that we can get values from the filtered QuerySet
# print("\nTesting value() on filtered QuerySet...")
# try:
#     filtered_values = smaller_excluded.get_last_values()
#     print(filtered_values.head(10))
#     print(f"Successfully retrieved {len(filtered_values)} value rows from filtered QuerySet")
#     if not filtered_values.empty:
#         print(f"Unique series codes: {filtered_values['series_code'].nunique()}")
# except Exception as e:
#     print(f"Note: Could not retrieve values (may need data loaded): {e}")

# # Example 10: Test mixing filter() and filter_exclude()
# print_separator("Example 10: Test mixing filter() and filter_exclude()")
# mixed_dataset = data_api.get(asset_class=["Equity", "Commodity", "Fixed Income"])
# print(f"Initial: {len(mixed_dataset.info())} series")

# # First filter (include)
# filtered = mixed_dataset.filter(asset_class=["Equity", "Commodity"])
# print(f"After filter(asset_class=['Equity', 'Commodity']): {len(filtered.info())} series")

# # Then exclude
# excluded = filtered.filter_exclude(region="North America")
# excluded_info = excluded.info()
# print(f"After filter_exclude(region='North America'): {len(excluded_info)} series")
# print(f"Regions: {sorted(excluded_info['region'].dropna().unique())}")
# print(f"Asset classes: {sorted(excluded_info['asset_class'].unique())}")

print_separator("All tests completed!")
