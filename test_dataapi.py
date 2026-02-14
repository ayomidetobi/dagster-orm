#!/usr/bin/env python3
"""Test script for DataAPI get(), info(), and value() methods.

This script tests the DataAPI class methods:
- get(): Create QuerySet with metadata filters
- .info(): Get metadata information for matching series
- .value(): Get value data for matching series

Usage:
    python test_dataapi.py
"""

import sys

import pandas as pd
from decouple import config

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource


def init_duckdb_from_env() -> DuckDBResource:
    """Initialize DuckDB resource from environment variables.

    Uses the same configuration as Dagster (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION).

    Returns:
        Initialized DuckDBResource instance

    Raises:
        ValueError: If required environment variables are missing
    """
    duckdb_cacher = duckdb_datacacher(
        bucket=config("S3_BUCKET", default=None),
        access_key=config("S3_ACCESS_KEY", default=None),
        secret_key=config("S3_SECRET_KEY", default=None),
        region=config("S3_REGION", default=None),
    )

    duckdb_resource = DuckDBResource(cacher=duckdb_cacher)
    duckdb_resource.setup_for_execution(None)

    return duckdb_resource


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_dataframe_summary(df: pd.DataFrame, name: str, max_rows: int = 10) -> None:
    """Print a summary of a DataFrame."""
    print(f"\n{name}:")
    print(f"  Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"  Columns: {', '.join(df.columns)}")

    if df.empty:
        print("  ⚠️  DataFrame is empty")
        return

    print(f"\n  First {min(max_rows, len(df))} rows:")
    print(df.head(max_rows).to_string(index=False))

    if len(df) > max_rows:
        print(f"\n  ... and {len(df) - max_rows} more rows")


def test_get_with_filters() -> None:
    """Test DataAPI.get() method with various filters."""
    print_section("Test 1: DataAPI.get() with filters")

    try:
        # Initialize DuckDB resource
        print("\n📦 Initializing DuckDB resource...")
        duckdb_resource = init_duckdb_from_env()
        print("✅ DuckDB resource initialized")

        # Create DataAPI instance
        print("\n📦 Creating DataAPI instance...")
        data_api = DataAPI(duckdb_resource)
        print("✅ DataAPI instance created")

        # Test 1.1: get() with single filter value
        print("\n🔍 Test 1.1: get() with single filter (asset_class='Equity')")
        try:
            dataset = data_api.get(asset_class="Equity")
            print("✅ QuerySet created successfully")
            print(f"   QuerySet type: {type(dataset)}")
        except Exception as e:
            print(f"❌ Failed to create QuerySet: {e}")
            raise

        # Test 1.2: get() with list filter values
        print("\n🔍 Test 1.2: get() with list filter (asset_class=['Equity', 'Commodity'])")
        try:
            dataset = data_api.get(asset_class=["Equity", "Commodity"])
            print("✅ QuerySet created successfully with list filter")
        except Exception as e:
            print(f"❌ Failed to create QuerySet with list: {e}")
            raise

        # Test 1.3: get() with multiple filters
        print(
            "\n🔍 Test 1.3: get() with multiple filters (asset_class='Equity', region='North America')"
        )
        try:
            dataset = data_api.get(asset_class="Equity", region="North America")
            print("✅ QuerySet created successfully with multiple filters")
        except Exception as e:
            print(f"❌ Failed to create QuerySet with multiple filters: {e}")
            raise

        # Test 1.4: get() with no filters (should return all)
        print("\n🔍 Test 1.4: get() with no filters (empty dict)")
        try:
            dataset = data_api.get()
            print("✅ QuerySet created successfully with no filters")
        except Exception as e:
            print(f"❌ Failed to create QuerySet with no filters: {e}")
            raise

        print("\n✅ All get() tests passed!")

    except Exception as e:
        print(f"\n❌ Error in test_get_with_filters: {e}")
        import traceback

        traceback.print_exc()
        raise


def test_info_method() -> None:
    """Test QuerySet.info() method."""
    print_section("Test 2: QuerySet.info() method")

    try:
        # Initialize DuckDB resource
        print("\n📦 Initializing DuckDB resource...")
        duckdb_resource = init_duckdb_from_env()
        print("✅ DuckDB resource initialized")

        # Create DataAPI instance
        print("\n📦 Creating DataAPI instance...")
        data_api = DataAPI(duckdb_resource)
        print("✅ DataAPI instance created")

        # Test 2.1: info() with single filter
        print("\n🔍 Test 2.1: info() with asset_class='Equity'")
        try:
            dataset = data_api.get(asset_class="Equity")
            metadata_df = dataset.info()
            print("✅ info() executed successfully")
            print_dataframe_summary(metadata_df, "Metadata DataFrame")
        except Exception as e:
            print(f"❌ Failed to get info(): {e}")
            raise

        # Test 2.2: info() with multiple filters
        print("\n🔍 Test 2.2: info() with asset_class='Equity', region='North America'")
        try:
            dataset = data_api.get(asset_class="Equity", region="North America")
            metadata_df = dataset.info()
            print("✅ info() executed successfully with multiple filters")
            print_dataframe_summary(metadata_df, "Metadata DataFrame")
        except Exception as e:
            print(f"❌ Failed to get info() with multiple filters: {e}")
            raise

        # Test 2.3: info() with list filters
        print("\n🔍 Test 2.3: info() with asset_class=['Equity', 'Commodity']")
        try:
            dataset = data_api.get(asset_class=["Equity", "Commodity"])
            metadata_df = dataset.info()
            print("✅ info() executed successfully with list filters")
            print_dataframe_summary(metadata_df, "Metadata DataFrame")
        except Exception as e:
            print(f"❌ Failed to get info() with list filters: {e}")
            raise

        # Test 2.4: info() with no filters
        print("\n🔍 Test 2.4: info() with no filters")
        try:
            dataset = data_api.get()
            metadata_df = dataset.info()
            print("✅ info() executed successfully with no filters")
            print_dataframe_summary(metadata_df, "Metadata DataFrame (all metadata)")
        except Exception as e:
            print(f"❌ Failed to get info() with no filters: {e}")
            raise

        print("\n✅ All info() tests passed!")

    except Exception as e:
        print(f"\n❌ Error in test_info_method: {e}")
        import traceback

        traceback.print_exc()
        raise


def test_value_method() -> None:
    """Test QuerySet.value() method."""
    print_section("Test 3: QuerySet.value() method")

    try:
        # Initialize DuckDB resource
        print("\n📦 Initializing DuckDB resource...")
        duckdb_resource = init_duckdb_from_env()
        print("✅ DuckDB resource initialized")

        # Create DataAPI instance
        print("\n📦 Creating DataAPI instance...")
        data_api = DataAPI(duckdb_resource)
        print("✅ DataAPI instance created")

        # Get a dataset with filters to ensure we have series codes
        print("\n🔍 Setting up dataset with filters...")
        dataset = data_api.get(asset_class="Currency")
        metadata_df = dataset.info()

        if metadata_df.empty:
            print("⚠️  No metadata found with filters. Trying without filters...")
            dataset = data_api.get()
            metadata_df = dataset.info()

        if metadata_df.empty:
            print("❌ No metadata available for testing")
            return

        print(f"✅ Found {len(metadata_df)} series in metadata")

        # Test 3.1: value() with no parameters
        print("\n🔍 Test 3.1: value() with no parameters")
        try:
            value_df = dataset.value()
            print("✅ value() executed successfully")
            print_dataframe_summary(value_df, "Value DataFrame", max_rows=5)
        except Exception as e:
            print(f"❌ Failed to get value(): {e}")
            raise

        # Test 3.2: value() with start date
        print("\n🔍 Test 3.2: value() with start date only")
        try:
            params = ValueQueryParams(start="2024-01-01")
            value_df = dataset.value(params=params)
            print("✅ value() executed successfully with start date")
            print_dataframe_summary(value_df, "Value DataFrame (filtered by start)", max_rows=5)
        except Exception as e:
            print(f"❌ Failed to get value() with start date: {e}")
            raise

        # Test 3.3: value() with end date
        print("\n🔍 Test 3.3: value() with end date only")
        try:
            params = ValueQueryParams(end="2024-12-31")
            value_df = dataset.value(params=params)
            print("✅ value() executed successfully with end date")
            print_dataframe_summary(value_df, "Value DataFrame (filtered by end)", max_rows=5)
        except Exception as e:
            print(f"❌ Failed to get value() with end date: {e}")
            raise

        # Test 3.4: value() with start and end dates
        print("\n🔍 Test 3.4: value() with start and end dates")
        try:
            params = ValueQueryParams(start="2024-01-01", end="2024-12-31")
            value_df = dataset.value(params=params)
            print("✅ value() executed successfully with date range")
            print_dataframe_summary(
                value_df, "Value DataFrame (filtered by date range)", max_rows=5
            )
        except Exception as e:
            print(f"❌ Failed to get value() with date range: {e}")
            raise

        # Test 3.5: value() with limit
        print("\n🔍 Test 3.5: value() with limit")
        try:
            params = ValueQueryParams(limit=10)
            value_df = dataset.value(params=params)
            print("✅ value() executed successfully with limit")
            print_dataframe_summary(value_df, "Value DataFrame (limited to 10 rows)", max_rows=10)
            if len(value_df) > 10:
                print(f"⚠️  Expected max 10 rows, got {len(value_df)}")
            else:
                print(f"✅ Limit applied correctly: {len(value_df)} rows")
        except Exception as e:
            print(f"❌ Failed to get value() with limit: {e}")
            raise

        # Test 3.6: value() with all parameters
        print("\n🔍 Test 3.6: value() with all parameters (start, end, limit, order_by)")
        try:
            params = ValueQueryParams(
                start="2024-01-01", end="2024-12-31", limit=5, order_by="timestamp"
            )
            value_df = dataset.value(params=params)
            print("✅ value() executed successfully with all parameters")
            print_dataframe_summary(value_df, "Value DataFrame (all params)", max_rows=5)
        except Exception as e:
            print(f"❌ Failed to get value() with all parameters: {e}")
            raise

        print("\n✅ All value() tests passed!")

    except Exception as e:
        print(f"\n❌ Error in test_value_method: {e}")
        import traceback

        traceback.print_exc()
        raise


def test_integration() -> None:
    """Test the full workflow: get() -> info() -> value()."""
    print_section("Test 4: Integration Test (get -> info -> value)")

    try:
        # Initialize DuckDB resource
        print("\n📦 Initializing DuckDB resource...")
        duckdb_resource = init_duckdb_from_env()
        print("✅ DuckDB resource initialized")

        # Create DataAPI instance
        print("\n📦 Creating DataAPI instance...")
        data_api = DataAPI(duckdb_resource)
        print("✅ DataAPI instance created")

        # Full workflow
        print("\n🔍 Step 1: Create QuerySet with filters")
        dataset = data_api.get(asset_class="Currency", region="North America")
        print("✅ QuerySet created")

        print("\n🔍 Step 2: Get metadata info")
        metadata_df = dataset.info()
        print(f"✅ Retrieved metadata for {len(metadata_df)} series")
        print_dataframe_summary(metadata_df, "Metadata", max_rows=5)

        if metadata_df.empty:
            print("⚠️  No metadata found. Trying without region filter...")
            dataset = data_api.get(asset_class="Currency")
            metadata_df = dataset.info()
            print(f"✅ Retrieved metadata for {len(metadata_df)} series (without region filter)")

        print("\n🔍 Step 3: Get value data")
        params = ValueQueryParams(start="2024-01-01", end="2024-12-31", limit=20)
        value_df = dataset.value(params=params)
        print(f"✅ Retrieved value data: {len(value_df)} rows")
        print_dataframe_summary(value_df, "Value Data", max_rows=10)

        print("\n✅ Integration test passed!")

    except Exception as e:
        print(f"\n❌ Error in integration test: {e}")
        import traceback

        traceback.print_exc()
        raise


def main() -> None:
    """Run all tests."""
    print("\n" + "=" * 80)
    print("  DataAPI Test Suite")
    print("  Testing: get(), info(), and value() methods")
    print("=" * 80)

    tests_passed = 0
    tests_failed = 0

    test_functions = [
        ("get() method", test_get_with_filters),
        ("info() method", test_info_method),
        ("value() method", test_value_method),
        ("Integration test", test_integration),
    ]

    for test_name, test_func in test_functions:
        try:
            test_func()
            tests_passed += 1
            print(f"\n✅ {test_name} completed successfully")
        except Exception as e:
            tests_failed += 1
            print(f"\n❌ {test_name} failed: {e}")

    # Summary
    print_section("Test Summary")
    print(f"\n✅ Tests passed: {tests_passed}")
    print(f"❌ Tests failed: {tests_failed}")
    print(f"📊 Total tests: {len(test_functions)}")

    if tests_failed == 0:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print(f"\n⚠️  {tests_failed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
