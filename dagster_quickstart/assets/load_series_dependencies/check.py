"""Asset check for validating parent series count matches calc_type requirements."""

import pandas as pd
from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.s3_paths import build_s3_control_table_path
from dagster_quickstart.orm.schema import ControlTableType
from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES
from dagster_quickstart.orm.schema.sql_scripts import VALIDATE_PARENT_SERIES_COUNT_QUERY

MAX_ERROR_DETAILS = 10
MAX_METADATA_ROWS = 20


def _build_calc_type_case_statement() -> str:
    """Build CASE statement for mapping calc_type to required_count.

    Returns:
        SQL CASE statement string for required_count calculation
    """
    when_clauses = [
        f"WHEN UPPER(COALESCE(calc_type, '')) = '{calc_type}' THEN {required_count}"
        for calc_type, required_count in CALCULATION_FORMULA_TYPES.items()
    ]
    return " ".join(when_clauses)


def _build_validation_sql(parquet_source: str) -> str:
    """Build SQL query for validating parent series counts.

    Args:
        parquet_source: SQL expression for parquet source (e.g., read_parquet('uri'))

    Returns:
        Complete SQL validation query string
    """
    case_statements = _build_calc_type_case_statement()
    return VALIDATE_PARENT_SERIES_COUNT_QUERY.format(
        case_statements=case_statements, parquet_source=parquet_source
    )


def _build_error_summary(invalid_df: pd.DataFrame, invalid_count: int) -> str:
    """Build error summary string from invalid rows.

    Args:
        invalid_df: DataFrame containing invalid rows
        invalid_count: Total number of invalid rows

    Returns:
        Formatted error summary string
    """
    error_details = []
    for _, row in invalid_df.head(MAX_ERROR_DETAILS).iterrows():
        required_str = (
            "unknown" if row["required_count"] is None else str(int(row["required_count"]))
        )
        error_details.append(
            f"{row['child_series_code']}: {row['error']} "
            f"(calc_type={row['calc_type']}, found={row['parent_count']}, required={required_str})"
        )

    error_summary = "; ".join(error_details)
    if invalid_count > MAX_ERROR_DETAILS:
        error_summary += f"; ... and {invalid_count - MAX_ERROR_DETAILS} more invalid row(s)"

    return error_summary


def _log_validation_errors(context: AssetCheckExecutionContext, invalid_df: pd.DataFrame) -> None:
    """Log detailed validation errors.

    Args:
        context: Dagster execution context for logging
        invalid_df: DataFrame containing invalid rows
    """
    for _, row in invalid_df.iterrows():
        context.log.error(f"Child series {row['child_series_code']}: {row['error']}")


@asset_check(
    asset="load_series_dependencies_to_s3",
    name="validate_parent_series_count",
    description="Validates that each calc_type has the required number of parent series",
    required_resource_keys={"duckdb"},
)
def validate_parent_series_count(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Validate that parent_series_code count matches the required count for each calc_type.

    The parent_series_code column contains pipe-separated values (e.g., "AAPL|MSFT|GOOGL").
    We count the number of parent series by splitting on '|' and validate against
    CALCULATION_FORMULA_TYPES requirements:
    - SPREAD: 2 parent series
    - FLY: 3 parent series
    - BOX: 4 parent series
    - RATIO: 2 parent series
    """
    duckdb_resource = context.resources.duckdb
    duckdb_repo = DuckDbRepository(duckdb_resource._con)
    s3_adapter = S3Adapter(duckdb_resource.get_bucket())
    parquet_adapter = ParquetAdapter()

    relative_path = build_s3_control_table_path(ControlTableType.SERIES_DEPENDENCIES.value)
    s3_uri = s3_adapter.get_relative_path_uri(relative_path)

    try:
        parquet_source = parquet_adapter.build_parquet_source(s3_uri)
        validation_sql = _build_validation_sql(parquet_source)
        invalid_df = duckdb_repo.execute_raw_sql(validation_sql)
        total_count = duckdb_repo.count_from_parquet(parquet_source)

        if total_count == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Series dependencies file is empty",
                metadata={"s3_uri": s3_uri},
            )

        invalid_count = len(invalid_df)

        if invalid_count > 0:
            error_summary = _build_error_summary(invalid_df, invalid_count)
            description = (
                f"Found {invalid_count} row(s) with incorrect parent series count. "
                f"Errors: {error_summary}"
            )

            context.log.error(f"Validation failed: {description}")
            _log_validation_errors(context, invalid_df)

            invalid_details = invalid_df.head(MAX_METADATA_ROWS)[
                ["child_series_code", "calc_type", "parent_count", "required_count", "error"]
            ].to_dict("records")

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=description,
                metadata={
                    "invalid_count": invalid_count,
                    "total_count": total_count,
                    "invalid_details": invalid_details,
                    "s3_uri": s3_uri,
                },
            )

        return AssetCheckResult(
            passed=True,
            description=f"All {total_count} series dependency row(s) have correct parent series count",
            metadata={
                "total_count": total_count,
                "invalid_count": 0,
                "s3_uri": s3_uri,
            },
        )

    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Validation failed with error: {exc!s}",
            metadata={
                "error": str(exc),
                "s3_uri": s3_uri,
            },
        )
