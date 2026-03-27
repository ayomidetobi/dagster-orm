"""Asset for calculating derived series from parent series.

Uses ORM layer (DataAPI) for all operations - no raw SQL.
Reads metadata_derived (series dependency definitions) from S3, loads parent series data,
calculates derived values based on calc_type, and saves to S3.
"""

from typing import Any, Callable, Dict, List

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.derived.config import DerivedConfig
from dagster_quickstart.assets.derived.partitions import DERIVED_CALC_PARTITIONS
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.s3_paths import build_s3_control_table_path
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, TickerSource, ValueColumns
from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES

# (sub, cols) -> series aligned to sub.index; ``sub`` is parent columns with all-null rows dropped.
PartitionCalculator = Callable[[pd.DataFrame, List[str]], pd.Series]


def _calc_spread_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[0]] - sub[cols[1]]


def _calc_fly_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[0]] - 2.0 * sub[cols[1]] + sub[cols[2]]


def _calc_box_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return (
        sub[cols[0]] - sub[cols[1]] - sub[cols[2]] + sub[cols[3]]
    )


def _calc_ratio_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    denom = sub[cols[1]]
    out = sub[cols[0]].div(denom)
    return out.mask(denom == 0)


def _calc_spread_inv_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[1]] - sub[cols[0]]


def _calc_ratio_inv_partition(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    denom = sub[cols[0]]
    out = sub[cols[1]].div(denom)
    return out.mask(denom == 0)


_PARTITION_CALCULATORS: Dict[str, PartitionCalculator] = {
    "SPREAD": _calc_spread_partition,
    "FLY": _calc_fly_partition,
    "BOX": _calc_box_partition,
    "RATIO": _calc_ratio_partition,
    "SPREAD_INV": _calc_spread_inv_partition,
    "RATIO_INV": _calc_ratio_inv_partition,
}

if set(_PARTITION_CALCULATORS) != set(CALCULATION_FORMULA_TYPES):
    raise RuntimeError(
        "_PARTITION_CALCULATORS keys must match CALCULATION_FORMULA_TYPES keys exactly"
    )


def _compute_derived_for_partition(
    partition_key: str,
    parent_pivot: pd.DataFrame,
    parent_series_codes: List[str],
) -> pd.Series:
    """Apply the vectorized formula for this Dagster partition only."""
    calculator = _PARTITION_CALCULATORS.get(partition_key)
    if calculator is None:
        raise ValueError(f"No partition calculator registered for {partition_key!r}")

    cols = parent_series_codes
    sub = parent_pivot[cols].dropna(how="any")
    if sub.empty:
        return pd.Series(dtype="float64")

    out = calculator(sub, cols)
    out = out.astype("float64")
    out.name = None
    return out


@asset(
    partitions_def=DERIVED_CALC_PARTITIONS,
    required_resource_keys={"duckdb"},
    name="calculate_derived_series",
)
def calculate_derived_series(
    context: AssetExecutionContext, config: DerivedConfig
) -> MaterializeResult:
    """Calculate derived series from parent series for one calculation partition.

    Each partition key uses its own vectorized calculator (see ``_PARTITION_CALCULATORS``).
    Only dependency rows whose ``calc_type`` matches the run partition are processed;
    results are written under ``field_type={partition_key}`` for INTERNAL.

    Args:
        context: Dagster asset execution context
        config: DerivedConfig with date range and configuration

    Returns:
        MaterializeResult with metadata about the calculated data
    """
    partition_key = context.partition_key

    duckdb_resource = context.resources.duckdb
    data_api = DataAPI(duckdb_resource)

    metadata_glob_relative = build_s3_control_table_path(config.control_table_type)
    dependencies_df = data_api.get(field_type=partition_key).info(allow_empty=True)

    if dependencies_df.empty:
        context.log.warning(
            f"No series dependencies found for partition {partition_key!r} "
            f"(metadata family glob {metadata_glob_relative})"
        )
        return MaterializeResult(
            metadata={
                "partition_key": partition_key,
                "num_records": 0,
                "series_processed": 0,
                "series_failed": 0,
                "s3_path": metadata_glob_relative,
            }
        )

    context.log.info(
        f"Loaded {len(dependencies_df)} series dependencies from {metadata_glob_relative}",
        extra={
            "dependencies_count": len(dependencies_df),
            "partition_key": partition_key,
        },
    )

    # One wide partition per materialization (matches Dagster partition_key).
    code_to_series: Dict[str, pd.Series] = {}
    series_processed = 0
    series_failed = 0
    failed_series: List[str] = []

    for _, row in dependencies_df.iterrows():
        series_code = row.get(MetadataColumns.SERIES_CODE)
        parent_series_code_str = row.get("parent_series_code", "")
        calc_type = row.get("calc_type", "")

        if not series_code or not parent_series_code_str or not calc_type:
            context.log.warning(
                f"Skipping row with missing data: series_code={series_code}, "
                f"parent={parent_series_code_str}, calc_type={calc_type}"
            )
            series_failed += 1
            continue

        # Parse parent series codes (pipe-separated)
        parent_series_codes = [
            code.strip() for code in parent_series_code_str.split("|") if code.strip()
        ]

        # Validate parent count matches calc_type requirement
        calc_type_upper = calc_type.upper()
        required_count = CALCULATION_FORMULA_TYPES.get(calc_type_upper)
        if required_count is None:
            context.log.error(
                f"Unknown calc_type '{calc_type}' for series {series_code}"
            )
            series_failed += 1
            failed_series.append(series_code)
            continue

        if len(parent_series_codes) != required_count:
            context.log.error(
                f"Series {series_code}: calc_type {calc_type} requires "
                f"{required_count} parent series, but found {len(parent_series_codes)}"
            )
            series_failed += 1
            failed_series.append(series_code)
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
                    f"No parent series data found for {series_code} "
                    f"in date range {config.start_date} to {config.end_date}"
                )
                series_failed += 1
                failed_series.append(series_code)
                continue

            # Pivot to wide format: timestamp as index, series_code as columns
            parent_pivot = parent_data.pivot(
                index=ValueColumns.TIMESTAMP,
                columns=ValueColumns.SERIES_CODE,
                values=ValueColumns.VALUE,
            )
            parent_pivot = parent_pivot.sort_index()

            derived_series = _compute_derived_for_partition(
                partition_key, parent_pivot, parent_series_codes
            )

            if derived_series.empty:
                context.log.warning(
                    f"No valid calculated values for {series_code} "
                    f"(missing parent data or invalid ratio rows)"
                )
                series_failed += 1
                failed_series.append(series_code)
                continue

            derived_series.name = str(series_code)
            code_to_series[str(series_code)] = derived_series
            series_processed += 1
            context.log.info(
                f"Calculated {len(derived_series)} values for {series_code} "
                f"(partition field_type={partition_key})"
            )

        except Exception as e:
            context.log.error(f"Error processing {series_code}: {e}", exc_info=True)
            series_failed += 1
            failed_series.append(series_code)

    # Save wide partition for this Dagster partition (field_type = calc formula name).
    total_data_points = 0
    partition_paths: list[str] = []
    write_stats_by_field: Dict[str, Any] = {}

    if code_to_series:
        data_api.validate_date_range_for_force_refresh(
            True, config.start_date, config.end_date
        )

        wide_df = pd.concat(code_to_series, axis=1)
        wide_df = wide_df.sort_index()
        wide_df.index.name = ValueColumns.TIMESTAMP

        write_stats = data_api.write_wide_value_partitions(
            wide_df=wide_df,
            field_type=partition_key,
            ticker_source=TickerSource.BLOOMBERG,
            start_date=config.start_date,
            end_date=config.end_date,
            force_refresh=True,
        )
        write_stats_by_field[partition_key] = write_stats
        partition_paths.extend(write_stats.get("written_relative_paths", []))
        total_data_points += int(wide_df.notna().to_numpy().sum())

    context.log.info(
        f"Processed {series_processed} series successfully, "
        f"{series_failed} failed. Saved {total_data_points} total data points.",
        extra={
            "series_processed": series_processed,
            "series_failed": series_failed,
            "total_data_points": total_data_points,
        },
    )

    metadata: Dict[str, Any] = {
        "partition_key": partition_key,
        "num_records": total_data_points,
        "series_processed": series_processed,
        "series_failed": series_failed,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "wide_partition_paths": MetadataValue.json(partition_paths),
        "write_stats_by_field_type": MetadataValue.json(write_stats_by_field),
    }

    if failed_series:
        metadata["failed_series"] = MetadataValue.json(failed_series[:20])  # Limit to 20

    return MaterializeResult(metadata=metadata)
