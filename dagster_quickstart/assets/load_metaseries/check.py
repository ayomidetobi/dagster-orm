"""Asset check reporting DuckLake metadata quality (rewrite DataAPI, snapshot-based).

Scoped to the series_codes in the current meta_series.csv -- pre-existing
catalog rows for series not in that file (e.g. old test/demo data, a
different file's series) are never reported. Duplicates/nulls are real
defects (ERROR). New columns/new column values are informational -- metadata
legitimately grows over time -- so they're surfaced as a WARN rather than
blocking the pipeline.
"""

from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_quickstart.assets.load_metaseries.config import LoadMetaSeriesConfig
from dagster_quickstart.assets.utils.duplicate_checks import log_duplicate_errors
from dagster_quickstart.rewrite.data_api.columns import MetadataColumns
from dagster_quickstart.rewrite.data_api.ingestion.file_loader import read_tabular_file


@asset_check(
    asset="load_meta_series_to_s3",
    name="validate_metadata_quality",
    description=(
        "Reports duplicate series_code rows, null values, and new columns/column values "
        "for the current meta_series.csv's series, diffed against DuckLake's own prior snapshot"
    ),
    required_resource_keys={"rewrite_data_api"},
)
def validate_metadata_quality(
    context: AssetCheckExecutionContext, config: LoadMetaSeriesConfig
) -> AssetCheckResult:
    """Report DuckLake metadata quality, scoped to the current source file's series_codes."""
    data_api = context.resources.rewrite_data_api.api

    try:
        source_frame = read_tabular_file(config.csv_path)
        series_codes = (
            source_frame[MetadataColumns.SERIES_CODE]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        report = data_api.get_metadata_quality_report(series_codes=series_codes)
    except Exception as exc:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Quality report failed with error: {exc!s}",
            metadata={"error": str(exc)},
        )

    context.log.info(f"Metadata quality report: {report.summary()}")

    if report.has_duplicates:
        log_duplicate_errors(
            context=context,
            duplicate_df=report.duplicate_series_codes,
            key_column="series_code",
            location_label="metadata",
        )
    if report.has_nulls:
        for column, count in report.null_counts.items():
            context.log.error(f"Column '{column}' has {count} null value(s)")
    if report.has_new_columns:
        context.log.info(f"New column(s) since last import: {report.new_columns}")
    if report.has_new_values:
        for column, values in report.new_values.items():
            context.log.info(f"New value(s) for column '{column}': {values}")

    metadata: dict[str, object] = {
        "total_count": report.total_count,
        "current_version": report.current_version,
        "baseline_version": report.baseline_version,
        "duplicate_count": len(report.duplicate_series_codes),
        "null_counts": report.null_counts,
        "new_columns": report.new_columns,
        "new_values": report.new_values,
    }
    if report.has_duplicates:
        metadata["duplicate_details"] = report.duplicate_series_codes.to_dict("records")

    if not report.is_clean:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Found data quality issues: {report.summary()}",
            metadata=metadata,
        )

    if report.has_new_columns or report.has_new_values:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Metadata changed since last import: {report.summary()}",
            metadata=metadata,
        )

    return AssetCheckResult(
        passed=True,
        description=f"Metadata is clean and unchanged since last import: {report.summary()}",
        metadata=metadata,
    )
