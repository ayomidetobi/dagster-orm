"""Asset for calculating derived series from parent series.

Uses ORM layer (DataAPI) for all operations - no raw SQL.
Reads series_dependencies from S3, loads parent series data,
calculates derived values based on calc_type, and saves to S3.
"""

from typing import Any, Dict, List

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.assets.derived.config import DerivedConfig
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.s3_paths import build_s3_control_table_path
from dagster_quickstart.orm.schema import TickerSource, ValueColumns
from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES


def _calculate_spread(parent_values: List[float]) -> float:
    """Calculate SPREAD: parent[0] - parent[1].

    Args:
        parent_values: List of parent series values (must have 2 elements)

    Returns:
        Calculated spread value
    """
    if len(parent_values) != 2:
        raise ValueError(f"SPREAD requires 2 parent series, got {len(parent_values)}")
    return parent_values[0] - parent_values[1]


def _calculate_fly(parent_values: List[float]) -> float:
    """Calculate FLY: parent[0] - 2*parent[1] + parent[2].

    Args:
        parent_values: List of parent series values (must have 3 elements)

    Returns:
        Calculated fly value
    """
    if len(parent_values) != 3:
        raise ValueError(f"FLY requires 3 parent series, got {len(parent_values)}")
    return parent_values[0] - 2 * parent_values[1] + parent_values[2]


def _calculate_box(parent_values: List[float]) -> float:
    """Calculate BOX: parent[0] - parent[1] - parent[2] + parent[3].

    Args:
        parent_values: List of parent series values (must have 4 elements)

    Returns:
        Calculated box value
    """
    if len(parent_values) != 4:
        raise ValueError(f"BOX requires 4 parent series, got {len(parent_values)}")
    return parent_values[0] - parent_values[1] - parent_values[2] + parent_values[3]


def _calculate_ratio(parent_values: List[float]) -> float:
    """Calculate RATIO: parent[0] / parent[1].

    Args:
        parent_values: List of parent series values (must have 2 elements)

    Returns:
        Calculated ratio value

    Raises:
        ValueError: If denominator is zero
    """
    if len(parent_values) != 2:
        raise ValueError(f"RATIO requires 2 parent series, got {len(parent_values)}")
    if parent_values[1] == 0:
        raise ValueError("RATIO denominator cannot be zero")
    return parent_values[0] / parent_values[1]


def _calculate_derived_value(calc_type: str, parent_values: List[float]) -> float:
    """Calculate derived value based on calculation type.

    Args:
        calc_type: Calculation type (SPREAD, FLY, BOX, RATIO)
        parent_values: List of parent series values

    Returns:
        Calculated derived value

    Raises:
        ValueError: If calc_type is unknown or invalid
    """
    calc_type_upper = calc_type.upper()
    if calc_type_upper == "SPREAD":
        return _calculate_spread(parent_values)
    elif calc_type_upper == "FLY":
        return _calculate_fly(parent_values)
    elif calc_type_upper == "BOX":
        return _calculate_box(parent_values)
    elif calc_type_upper == "RATIO":
        return _calculate_ratio(parent_values)
    else:
        raise ValueError(f"Unknown calculation type: {calc_type}")


@asset(
    required_resource_keys={"duckdb"},
    name="calculate_derived_series",
    deps=["ingest_bloomberg_data_daily"],
)
def calculate_derived_series(
    context: AssetExecutionContext, config: DerivedConfig
) -> MaterializeResult:
    """Calculate derived series from parent series based on calculation type.

    Reads series_dependencies from S3, loads parent series value data within
    the specified date range, calculates derived values, and saves to S3.

    Args:
        context: Dagster asset execution context
        config: DerivedConfig with date range and configuration

    Returns:
        MaterializeResult with metadata about the calculated data
    """
    duckdb_resource = context.resources.duckdb
    data_api = DataAPI(duckdb_resource)
    duckdb_repo = DuckDbRepository(duckdb_resource._con)
    s3_adapter = S3Adapter(duckdb_resource.get_bucket())
    parquet_adapter = ParquetAdapter()

    # Load series_dependencies from S3
    relative_path = build_s3_control_table_path(config.control_table_type)
    s3_uri = s3_adapter.get_relative_path_uri(relative_path)
    parquet_source = parquet_adapter.build_parquet_source(s3_uri)

    dependencies_query = QueryBuilder("_deps")
    dependencies_query.select("*")
    deps_sql = dependencies_query.build()[0].replace("FROM _deps", f"FROM {parquet_source}")
    dependencies_df = duckdb_repo.execute_raw_sql(deps_sql)

    if dependencies_df.empty:
        context.log.warning(f"No series dependencies found in {s3_uri}")
        return MaterializeResult(
            metadata={
                "num_records": 0,
                "series_processed": 0,
                "series_failed": 0,
                "s3_path": relative_path,
            }
        )

    context.log.info(
        f"Loaded {len(dependencies_df)} series dependencies from {s3_uri}",
        extra={"dependencies_count": len(dependencies_df)},
    )

    # Process each dependency and calculate derived values
    all_calculated_data: Dict[str, List[Dict[str, Any]]] = {}
    series_processed = 0
    series_failed = 0
    failed_series: List[str] = []

    for _, row in dependencies_df.iterrows():
        child_series_code = row.get("child_series_code")
        parent_series_code_str = row.get("parent_series_code", "")
        calc_type = row.get("calc_type", "")

        if not child_series_code or not parent_series_code_str or not calc_type:
            context.log.warning(
                f"Skipping row with missing data: child={child_series_code}, "
                f"parent={parent_series_code_str}, calc_type={calc_type}"
            )
            series_failed += 1
            continue

        # Parse parent series codes (pipe-separated)
        parent_series_codes = [
            code.strip() for code in parent_series_code_str.split("|") if code.strip()
        ]

        # Validate parent count matches calc_type requirement
        required_count = CALCULATION_FORMULA_TYPES.get(calc_type.upper())
        if required_count is None:
            context.log.error(
                f"Unknown calc_type '{calc_type}' for child series {child_series_code}"
            )
            series_failed += 1
            failed_series.append(child_series_code)
            continue

        if len(parent_series_codes) != required_count:
            context.log.error(
                f"Child series {child_series_code}: calc_type {calc_type} requires "
                f"{required_count} parent series, but found {len(parent_series_codes)}"
            )
            series_failed += 1
            failed_series.append(child_series_code)
            continue

        # Load parent series value data within date range
        try:
            parent_data = data_api._value_repository.get_batch_series_data(
                series_codes=parent_series_codes,
                tickersource=TickerSource.BLOOMBERG,
                start=config.start_date,
                end=config.end_date,
            )

            if parent_data.empty:
                context.log.warning(
                    f"No parent series data found for {child_series_code} "
                    f"in date range {config.start_date} to {config.end_date}"
                )
                series_failed += 1
                failed_series.append(child_series_code)
                continue

            # Pivot to wide format: timestamp as index, series_code as columns
            parent_pivot = parent_data.pivot(
                index=ValueColumns.TIMESTAMP,
                columns=ValueColumns.SERIES_CODE,
                values=ValueColumns.VALUE,
            )

            # Calculate derived values for each timestamp
            calculated_points: List[Dict[str, Any]] = []
            for timestamp, row_values in parent_pivot.iterrows():
                # Get values for each parent series in order
                parent_values = []
                for parent_code in parent_series_codes:
                    value = row_values.get(parent_code)
                    if pd.isna(value):
                        # Skip this timestamp if any parent value is missing
                        break
                    parent_values.append(float(value))

                # Only calculate if we have all parent values
                if len(parent_values) == required_count:
                    try:
                        calculated_value = _calculate_derived_value(calc_type, parent_values)
                        calculated_points.append(
                            {"timestamp": timestamp, "value": calculated_value}
                        )
                    except ValueError as e:
                        context.log.warning(
                            f"Failed to calculate {calc_type} for {child_series_code} "
                            f"at {timestamp}: {e}"
                        )

            if calculated_points:
                all_calculated_data[child_series_code] = calculated_points
                series_processed += 1
                context.log.info(
                    f"Calculated {len(calculated_points)} values for {child_series_code}"
                )
            else:
                context.log.warning(
                    f"No valid calculated values for {child_series_code} "
                    f"(missing parent data or calculation errors)"
                )
                series_failed += 1
                failed_series.append(child_series_code)

        except Exception as e:
            context.log.error(f"Error processing {child_series_code}: {e}", exc_info=True)
            series_failed += 1
            failed_series.append(child_series_code)

    # Save all calculated data to S3
    total_data_points = 0
    saved_paths: Dict[str, str] = {}

    if all_calculated_data:
        saved_paths = data_api.save_value_data_to_s3(
            data_points=all_calculated_data,
            ticker_source=TickerSource.INTERNAL,  # Derived series use INTERNAL ticker source
            force_refresh=True,
            start_date=config.start_date,
            end_date=config.end_date,
        )
        total_data_points = sum(len(points) for points in all_calculated_data.values())

    context.log.info(
        f"Processed {series_processed} series successfully, "
        f"{series_failed} failed. Saved {total_data_points} total data points.",
        extra={
            "series_processed": series_processed,
            "series_failed": series_failed,
            "total_data_points": total_data_points,
        },
    )

    metadata = {
        "num_records": total_data_points,
        "series_processed": series_processed,
        "series_failed": series_failed,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "s3_paths": MetadataValue.json(list(saved_paths.values())),
    }

    if failed_series:
        metadata["failed_series"] = MetadataValue.json(failed_series[:20])  # Limit to 20

    return MaterializeResult(metadata=metadata)
