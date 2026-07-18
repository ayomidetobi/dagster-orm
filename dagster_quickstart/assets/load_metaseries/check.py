"""Asset check for validating metadata parquet against wide-format lookup parquet."""

from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager
from dagster_quickstart.orm.schema import MetadataColumns, TableNames
from dagster_quickstart.assets.utils.duplicate_checks import (
    find_duplicate_keys,
    log_duplicate_errors,
)


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

        metadata_parquet_source = parquet_adapter.build_parquet_source(metadata_uri)
        total_count = duckdb_repo.count_from_parquet(metadata_parquet_source)

        # 2️⃣ Check for duplicate series_code values in metadata
        duplicate_df = find_duplicate_keys(
            duckdb_repo=duckdb_repo,
            parquet_source=metadata_parquet_source,
            column="series_code",
        )
        duplicate_count = len(duplicate_df)

        # 3️⃣ If any invalid rows or duplicates, fail the asset check with detailed information
        if invalid_count > 0 or duplicate_count > 0:
            parts = []
            if invalid_count > 0:
                parts.append(f"{invalid_count} invalid lookup value(s)")
            if duplicate_count > 0:
                parts.append(f"{duplicate_count} duplicate series_code value(s)")

            # Get unique series count from invalid rows (only for invalid values)
            unique_series = (
                invalid_df[MetadataColumns.SERIES_CODE].nunique() if invalid_count > 0 else 0
            )

            # Build detailed error messages for invalid values
            error_details = []
            for _, row in invalid_df.iterrows():
                error_details.append(
                    f"{row[MetadataColumns.SERIES_CODE]}: "
                    f"{row['invalid_column']}='{row['invalid_value']}'"
                )

            error_summary = "; ".join(error_details[:10])
            if invalid_count > 10:
                error_summary += f"; ... and {invalid_count - 10} more invalid value(s)"

            description = "Found " + " and ".join(parts) + "."
            if invalid_count > 0:
                description += (
                    f" Invalid lookup values across {unique_series} series: " f"{error_summary}"
                )

            # Build duplicate summary
            duplicate_details = None
            if duplicate_count > 0:
                duplicate_examples = []
                for _, row in duplicate_df.head(10).iterrows():
                    duplicate_examples.append(
                        f"{row['series_code']} (occurrences={int(row['duplicate_count'])})"
                    )
                duplicate_summary = "; ".join(duplicate_examples)
                description += f" Duplicate series_code values: {duplicate_summary}"

                duplicate_details = duplicate_df[["series_code", "duplicate_count"]].to_dict(
                    "records"
                )

            # Log detailed errors
            context.log.error(f"Validation failed: {description}")
            for _, row in invalid_df.iterrows():
                context.log.error(
                    f"Series {row[MetadataColumns.SERIES_CODE]}: "
                    f"Column '{row['invalid_column']}' has invalid value "
                    f"'{row['invalid_value']}'"
                )
            if duplicate_count > 0:
                log_duplicate_errors(
                    context=context,
                    duplicate_df=duplicate_df,
                    key_column="series_code",
                    location_label="metadata",
                )

            metadata: dict[str, object] = {
                "invalid_count": invalid_count,
                "duplicate_count": duplicate_count,
                "total_count": total_count,
                "metadata_uri": metadata_uri,
                "lookup_uri": lookup_uri,
            }

            if invalid_count > 0:
                metadata.update(
                    {
                        "unique_series_count": int(unique_series),
                        "invalid_details": invalid_df.to_dict("records"),
                        "invalid_series_codes": invalid_df[MetadataColumns.SERIES_CODE]
                        .unique()
                        .tolist(),
                    }
                )

            if duplicate_details is not None:
                metadata["duplicate_details"] = duplicate_details

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=description,
                metadata=metadata,
            )

        # 4️⃣ If all rows are valid and no duplicates
        return AssetCheckResult(
            passed=True,
            description=(
                f"All {total_count} metadata row(s) validated successfully "
                "against lookup table with no duplicate series_code values"
            ),
            metadata={
                "total_count": total_count,
                "invalid_count": 0,
                "duplicate_count": 0,
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
