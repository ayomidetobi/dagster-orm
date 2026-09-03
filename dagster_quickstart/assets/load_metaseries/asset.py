"""Asset for loading meta series CSV into the DuckLake-backed rewrite data lake.

Uses the new rewrite DataAPI (rewrite/data_api/, incl. rewrite/data_api/ingestion/)
for materialization -- metadata lands straight in the DuckLake metadata table, with
no separate S3 parquet control-table copy. Data quality is validated in-process via
a check_spec (see check.py's build_metadata_quality_check_result), directly against
the validated_df import_metadata() returns -- no second CSV read or DuckLake query.
"""

from dagster import AssetCheckSpec, AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.load_metaseries.check import (
    CHECK_NAME,
    build_metadata_quality_check_result,
)
from dagster_quickstart.assets.load_metaseries.config import LoadMetaSeriesConfig


@asset(
    required_resource_keys={"rewrite_data_api"},
    name="load_meta_series_to_s3",
    deps=["load_lookup_tables_to_s3"],
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="load_meta_series_to_s3",
            description=(
                "Validates the just-imported metadata rows with pandera -- "
                "null/blank/duplicate series_code and series_name, paired "
                "Bloomberg ticker/field, and valid_from <= valid_to"
            ),
        )
    ],
)
def load_meta_series_to_s3(context: AssetExecutionContext, config: LoadMetaSeriesConfig):
    """Load meta series CSV into the DuckLake metadata table.

    Args:
        context: Dagster asset execution context
        config: LoadMetaSeriesConfig with asset configuration

    Yields:
        AssetCheckResult for validate_metadata_quality, then a MaterializeResult
        with metadata about the loaded data.
    """
    data_api = context.resources.rewrite_data_api.api
    validated_df = data_api.import_metadata(path=config.csv_path, fresh=config.fresh)
    row_count = len(validated_df)

    context.log.info(f"Loaded {row_count} meta series rows into the DuckLake metadata table")

    yield build_metadata_quality_check_result(validated_df, log=context.log)

    preview_columns = [column for column in config.preview_columns if column in validated_df.columns]
    preview_df = validated_df[preview_columns].head(config.preview_limit)

    yield MaterializeResult(
        metadata={
            "num_records": row_count,
            "preview": MetadataValue.md(preview_df.to_markdown()),
        }
    )
