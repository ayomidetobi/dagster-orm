"""Asset check for validating metadata parquet against wide-format lookup parquet."""

from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.schema import MetadataColumns, TableNames


@asset_check(
    asset="load_meta_series_to_s3",
    name="validate_metadata_against_lookup",
    description="Validates that all metadata rows exist in lookup table using SQL semi-join (wide-format lookup)",
    required_resource_keys={"duckdb"},
)
def validate_metadata_against_lookup(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Validate metadata parquet against wide-format lookup parquet using SQL-only validation."""
    duckdb_resource = context.resources.duckdb

    # Initialize repository with dependency injection
    duckdb_repo = DuckDbRepository(duckdb_resource._con)
    parquet_adapter = ParquetAdapter()
    s3_adapter = S3Adapter(duckdb_resource.get_bucket())
    temp_table_manager = TempTableManager(duckdb_repo)

    validation_repo = ValidationRepository(
        duckdb_repository=duckdb_repo,
        parquet_adapter=parquet_adapter,
        s3_adapter=s3_adapter,
        temp_table_manager=temp_table_manager,
    )

    metadata_uri = s3_adapter.get_metadata_uri(TableNames.METADATA)
    lookup_uri = s3_adapter.get_lookup_uri()

    try:
        # 1️⃣ Get invalid rows (only series_code and series_name)
        invalid_df = validation_repo.get_invalid_rows(
            filters=None,  # or pass specific filters if needed
            control_type=TableNames.METADATA,
        )

        invalid_count = len(invalid_df)

        # 2️⃣ If any invalid rows, fail the asset check with detailed information
        if invalid_count > 0:
            # Get unique series count
            unique_series = invalid_df[MetadataColumns.SERIES_CODE].nunique()

            # Build detailed error messages
            error_details = []
            for _, row in invalid_df.iterrows():
                error_details.append(
                    f"{row[MetadataColumns.SERIES_CODE]}: {row['invalid_column']}='{row['invalid_value']}'"
                )

            # Create summary description
            error_summary = "; ".join(error_details[:10])
            if invalid_count > 10:
                error_summary += f"; ... and {invalid_count - 10} more invalid value(s)"

            description = (
                f"Found {invalid_count} invalid lookup value(s) across {unique_series} series. "
                f"Invalid values: {error_summary}"
            )

            # Log detailed errors
            context.log.error(f"Validation failed: {description}")
            for _, row in invalid_df.iterrows():
                context.log.error(
                    f"Series {row[MetadataColumns.SERIES_CODE]}: "
                    f"Column '{row['invalid_column']}' has invalid value '{row['invalid_value']}'"
                )

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=description,
                metadata={
                    "invalid_count": invalid_count,
                    "unique_series_count": int(unique_series),
                    "invalid_details": invalid_df.to_dict("records"),
                    "invalid_series_codes": invalid_df[MetadataColumns.SERIES_CODE]
                    .unique()
                    .tolist(),
                    "metadata_uri": metadata_uri,
                    "lookup_uri": lookup_uri,
                },
            )

        # 3️⃣ If all rows are valid
        metadata_parquet_source = parquet_adapter.build_parquet_source(metadata_uri)
        total_count = duckdb_repo.count_from_parquet(metadata_parquet_source)

        return AssetCheckResult(
            passed=True,
            description=f"All {total_count} metadata row(s) validated successfully against lookup table",
            metadata={
                "total_count": total_count,
                "invalid_count": 0,
                "metadata_uri": metadata_uri,
                "lookup_uri": lookup_uri,
            },
        )

    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Validation failed with error: {exc!s}",
            metadata={
                "error": str(exc),
                "metadata_uri": metadata_uri,
                "lookup_uri": lookup_uri,
            },
        )
