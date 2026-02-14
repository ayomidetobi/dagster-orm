"""Pytest tests for DataAPI get(), info(), and value() methods.

This test suite tests the DataAPI class methods:
- get(): Create QuerySet with metadata filters
- .info(): Get metadata information for matching series
- .value(): Get value data for matching series

Usage:
    pytest test_dataapi.py                    # Run all tests
    pytest test_dataapi.py::test_integration  # Run only integration test
    pytest test_dataapi.py -k "get"           # Run tests matching "get"
"""

import logging

import pandas as pd
import pytest
from decouple import config

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource

# Configure logger
logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def duckdb_resource() -> DuckDBResource:
    """Initialize DuckDB resource from environment variables.

    Uses the same configuration as Dagster (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION).

    Returns:
        Initialized DuckDBResource instance

    Raises:
        ValueError: If required environment variables are missing
    """
    logger.info("Initializing DuckDB resource from environment variables")
    duckdb_cacher = duckdb_datacacher(
        bucket=config("S3_BUCKET", default=None),
        access_key=config("S3_ACCESS_KEY", default=None),
        secret_key=config("S3_SECRET_KEY", default=None),
        region=config("S3_REGION", default=None),
    )

    duckdb_resource = DuckDBResource(cacher=duckdb_cacher)
    duckdb_resource.setup_for_execution(None)
    logger.info("DuckDB resource initialized successfully")
    return duckdb_resource


@pytest.fixture(scope="class")
def data_api(duckdb_resource: DuckDBResource) -> DataAPI:
    """Create DataAPI instance for testing.

    Args:
        duckdb_resource: DuckDBResource fixture

    Returns:
        DataAPI instance
    """
    logger.info("Creating DataAPI instance")
    api = DataAPI(duckdb_resource)
    logger.info("DataAPI instance created successfully")
    return api


def log_dataframe_summary(df: pd.DataFrame, name: str, max_rows: int = 10) -> None:
    """Log a summary of a DataFrame.

    Args:
        df: DataFrame to summarize
        name: Name/description of the DataFrame
        max_rows: Maximum number of rows to display
    """
    logger.info(f"{name}: Shape {df.shape[0]} rows x {df.shape[1]} columns")
    logger.info(f"{name}: Columns: {', '.join(df.columns)}")

    if df.empty:
        logger.warning(f"{name}: DataFrame is empty")
        return

    logger.info(f"{name}: First {min(max_rows, len(df))} rows:")
    logger.info(f"\n{df.head(max_rows).to_string(index=False)}")

    if len(df) > max_rows:
        logger.info(f"{name}: ... and {len(df) - max_rows} more rows")


class TestDataAPIGet:
    """Test DataAPI.get() method with various filters."""

    def test_get_single_filter(self, data_api: DataAPI) -> None:
        """Test get() with single filter value."""
        logger.info("Test: get() with single filter (asset_class='Equity')")
        dataset = data_api.get(asset_class="Equity")
        assert dataset is not None
        logger.info(f"QuerySet created successfully, type: {type(dataset)}")

    def test_get_list_filter(self, data_api: DataAPI) -> None:
        """Test get() with list filter values."""
        logger.info("Test: get() with list filter (asset_class=['Equity', 'Commodity'])")
        dataset = data_api.get(asset_class=["Equity", "Commodity"])
        assert dataset is not None
        logger.info("QuerySet created successfully with list filter")

    def test_get_multiple_filters(self, data_api: DataAPI) -> None:
        """Test get() with multiple filters."""
        logger.info(
            "Test: get() with multiple filters (asset_class='Equity', region='North America')"
        )
        dataset = data_api.get(asset_class="Equity", region="North America")
        assert dataset is not None
        logger.info("QuerySet created successfully with multiple filters")

    def test_get_no_filters(self, data_api: DataAPI) -> None:
        """Test get() with no filters (should return all)."""
        logger.info("Test: get() with no filters")
        dataset = data_api.get()
        assert dataset is not None
        logger.info("QuerySet created successfully with no filters")


class TestQuerySetInfo:
    """Test QuerySet.info() method."""

    def test_info_single_filter(self, data_api: DataAPI) -> None:
        """Test info() with single filter."""
        logger.info("Test: info() with asset_class='Equity'")
        dataset = data_api.get(asset_class="Equity")
        metadata_df = dataset.info()
        assert isinstance(metadata_df, pd.DataFrame)
        log_dataframe_summary(metadata_df, "Metadata DataFrame")
        logger.info("info() executed successfully")

    def test_info_multiple_filters(self, data_api: DataAPI) -> None:
        """Test info() with multiple filters."""
        logger.info("Test: info() with asset_class='Equity', region='North America'")
        dataset = data_api.get(asset_class="Equity", region="North America")
        metadata_df = dataset.info()
        assert isinstance(metadata_df, pd.DataFrame)
        log_dataframe_summary(metadata_df, "Metadata DataFrame")
        logger.info("info() executed successfully with multiple filters")

    def test_info_list_filters(self, data_api: DataAPI) -> None:
        """Test info() with list filters."""
        logger.info("Test: info() with asset_class=['Equity', 'Commodity']")
        dataset = data_api.get(asset_class=["Equity", "Commodity"])
        metadata_df = dataset.info()
        assert isinstance(metadata_df, pd.DataFrame)
        log_dataframe_summary(metadata_df, "Metadata DataFrame")
        logger.info("info() executed successfully with list filters")

    def test_info_no_filters(self, data_api: DataAPI) -> None:
        """Test info() with no filters."""
        logger.info("Test: info() with no filters")
        dataset = data_api.get()
        metadata_df = dataset.info()
        assert isinstance(metadata_df, pd.DataFrame)
        log_dataframe_summary(metadata_df, "Metadata DataFrame (all metadata)")
        logger.info("info() executed successfully with no filters")


@pytest.fixture
def dataset_with_series(data_api: DataAPI):
    """Create a dataset with series for value tests."""
    logger.info("Setting up dataset with filters for value tests")
    dataset = data_api.get(asset_class="Currency")
    metadata_df = dataset.info()

    if metadata_df.empty:
        logger.warning("No metadata found with filters. Trying without filters...")
        dataset = data_api.get()
        metadata_df = dataset.info()

    if metadata_df.empty:
        pytest.skip("No metadata available for testing")

    logger.info(f"Found {len(metadata_df)} series in metadata")
    return dataset


class TestQuerySetValue:
    """Test QuerySet.value() method."""

    def test_value_no_params(self, dataset_with_series) -> None:
        """Test value() with no parameters."""
        logger.info("Test: value() with no parameters")
        value_df = dataset_with_series.value()
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame", max_rows=5)
        logger.info("value() executed successfully")

    def test_value_start_date(self, dataset_with_series) -> None:
        """Test value() with start date only."""
        logger.info("Test: value() with start date only")
        params = ValueQueryParams(start="2024-01-01")
        value_df = dataset_with_series.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame (filtered by start)", max_rows=5)
        logger.info("value() executed successfully with start date")

    def test_value_end_date(self, dataset_with_series) -> None:
        """Test value() with end date only."""
        logger.info("Test: value() with end date only")
        params = ValueQueryParams(end="2024-12-31")
        value_df = dataset_with_series.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame (filtered by end)", max_rows=5)
        logger.info("value() executed successfully with end date")

    def test_value_date_range(self, dataset_with_series) -> None:
        """Test value() with start and end dates."""
        logger.info("Test: value() with start and end dates")
        params = ValueQueryParams(start="2024-01-01", end="2024-12-31")
        value_df = dataset_with_series.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame (filtered by date range)", max_rows=5)
        logger.info("value() executed successfully with date range")

    def test_value_limit(self, dataset_with_series) -> None:
        """Test value() with limit."""
        logger.info("Test: value() with limit")
        params = ValueQueryParams(limit=10)
        value_df = dataset_with_series.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame (limited to 10 rows)", max_rows=10)
        if len(value_df) > 10:
            logger.warning(f"Expected max 10 rows, got {len(value_df)}")
        else:
            logger.info(f"Limit applied correctly: {len(value_df)} rows")
        logger.info("value() executed successfully with limit")

    def test_value_all_params(self, dataset_with_series) -> None:
        """Test value() with all parameters (start, end, limit, order_by)."""
        logger.info("Test: value() with all parameters (start, end, limit, order_by)")
        params = ValueQueryParams(
            start="2024-01-01", end="2024-12-31", limit=5, order_by="timestamp"
        )
        value_df = dataset_with_series.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        log_dataframe_summary(value_df, "Value DataFrame (all params)", max_rows=5)
        logger.info("value() executed successfully with all parameters")


class TestIntegration:
    """Test the full workflow: get() -> info() -> value()."""

    def test_integration_workflow(self, data_api: DataAPI) -> None:
        """Test full integration workflow with multiple filters."""
        logger.info("Integration Test: Full workflow (get -> info -> value)")

        # Step 1: Create QuerySet with filters
        logger.info("Step 1: Create QuerySet with filters")
        dataset = data_api.get(asset_class="Commodity", product_type="Mutual Fund")
        assert dataset is not None
        logger.info("QuerySet created successfully")

        # Step 2: Get metadata info
        logger.info("Step 2: Get metadata info")
        metadata_df = dataset.info()
        assert isinstance(metadata_df, pd.DataFrame)
        logger.info(f"Retrieved metadata for {len(metadata_df)} series")
        log_dataframe_summary(metadata_df, "Metadata", max_rows=5)

        if metadata_df.empty:
            logger.warning("No metadata found. Trying with single filter...")
            dataset = data_api.get(asset_class="Currency")
            metadata_df = dataset.info()
            logger.info(f"Retrieved metadata for {len(metadata_df)} series (with single filter)")

        # Step 3: Get value data
        logger.info("Step 3: Get value data")
        params = ValueQueryParams(start="2024-01-01", end="2024-12-31", limit=20)
        value_df = dataset.value(params=params)
        assert isinstance(value_df, pd.DataFrame)
        logger.info(f"Retrieved value data: {len(value_df)} rows")
        log_dataframe_summary(value_df, "Value Data", max_rows=10)

        logger.info("Integration test completed successfully")
