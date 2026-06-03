"""Asset for calculating derived series from parent series.

Uses ORM layer (DataAPI) for all operations - no raw SQL.
Reads metadata_derived (series dependency definitions) from S3, loads parent series data,
calculates derived values based on calc_type, and saves to S3.
"""

from typing import Any, Dict, List

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.derived.config import DerivedConfig
from dagster_quickstart.assets.derived.partitions import DERIVED_CALC_PARTITIONS
from dagster_quickstart.orm.derived_calc import compute_derived_series, parse_parent_series_codes
from dagster_quickstart.orm.s3_paths import build_s3_control_table_path
from dagster_quickstart.orm.schema import MetadataColumns, TickerSource, ValueColumns
from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES


@asset(
    partitions_def=DERIVED_CALC_PARTITIONS,
    required_resource_keys={"data_api"},
    name="calculate_derived_series",
)
def calculate_derived_series(
    context: AssetExecutionContext, config: DerivedConfig
) -> MaterializeResult:
    """Calculate derived series from parent series for one calculation partition.

    Each partition key uses its own vectorized calculator (see ``derived_calc``).
    Only dependency rows whose ``calc_type`` matches the run partition are processed;
    results are written under ``field_type={partition_key}`` for INTERNAL.

    Args:
        context: Dagster asset execution context
        config: DerivedConfig with date range and configuration

    Returns:
        MaterializeResult with metadata about the calculated data
    """
    partition_key = context.partition_key

    data_api = context.resources.data_api.get_api()

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

    code_to_series: Dict[str, pd.Series] = {}
    series_processed = 0
    series_failed = 0
    failed_series: List[str] = []

    for _, row in dependencies_df.iterrows():
        series_code = row.get(MetadataColumns.SERIES_CODE)
        parent_series_code_str = row.get(MetadataColumns.PARENT_SERIES_CODE, "")
        calc_type = row.get(MetadataColumns.CALC_TYPE, "")

        if not series_code or not parent_series_code_str or not calc_type:
            context.log.warning(
                f"Skipping row with missing data: series_code={series_code}, "
                f"parent={parent_series_code_str}, calc_type={calc_type}"
            )
            series_failed += 1
            continue

        parent_series_codes = parse_parent_series_codes(parent_series_code_str)

        calc_type_upper = str(calc_type).upper()
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

            parent_pivot = parent_data.pivot(
                index=ValueColumns.TIMESTAMP,
                columns=ValueColumns.SERIES_CODE,
                values=ValueColumns.VALUE,
            )
            parent_pivot = parent_pivot.sort_index()

            derived_series = compute_derived_series(
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
        metadata["failed_series"] = MetadataValue.json(failed_series[:20])

    return MaterializeResult(metadata=metadata)
