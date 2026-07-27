"""Asset for loading meta series CSV into the DuckLake-backed rewrite data lake.

Uses the new rewrite DataAPI (rewrite/data_api/, incl. rewrite/data_api/ingestion/)
for materialization -- metadata lands straight in the DuckLake metadata table, with
no separate S3 parquet control-table copy. Data quality (duplicates, nulls, new
columns/values) is diffed straight off DuckLake's own snapshot history by
data_api.get_metadata_quality_report() -- see assets/load_metaseries/check.py.
"""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.load_metaseries.config import LoadMetaSeriesConfig


@asset(
    required_resource_keys={"rewrite_data_api"},
    name="load_meta_series_to_s3",
    deps=["load_lookup_tables_to_s3"],
)
def load_meta_series_to_s3(
    context: AssetExecutionContext, config: LoadMetaSeriesConfig
) -> MaterializeResult:
    """Load meta series CSV into the DuckLake metadata table.

    Args:
        context: Dagster asset execution context
        config: LoadMetaSeriesConfig with asset configuration

    Returns:
        MaterializeResult with metadata about the loaded data
    """
    data_api = context.resources.rewrite_data_api.api
    validated_df = data_api.import_metadata(path=config.csv_path, fresh=config.fresh)
    row_count = len(validated_df)

    report = data_api.get_metadata_quality_report(
        series_codes=validated_df["series_code"].tolist()
    )
    context.log.info(f"Loaded {row_count} meta series rows into the DuckLake metadata table")
    context.log.info(f"Metadata quality report: {report.summary()}")

    preview_columns = [column for column in config.preview_columns if column in validated_df.columns]
    preview_df = validated_df[preview_columns].head(config.preview_limit)

    return MaterializeResult(
        metadata={
            "num_records": row_count,
            "preview": MetadataValue.md(preview_df.to_markdown()),
            "quality_report": report.summary(),
            "duplicate_count": len(report.duplicate_series_codes),
            "null_counts": report.null_counts,
            "new_columns": report.new_columns,
            "new_values": report.new_values,
            "current_version": report.current_version,
            "baseline_version": report.baseline_version,
        }
    )
