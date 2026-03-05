"""Asset for loading series dependencies CSV to S3 as Parquet file.

Uses ORM layer (DataAPI) for all operations - no raw SQL.
Reads series_dependencies.csv and saves to S3.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.assets.load_series_dependencies.config import (
    LoadSeriesDependenciesConfig,
)
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.s3_paths import build_s3_control_table_path


@asset(
    required_resource_keys={"duckdb"},
    name="load_series_dependencies_to_s3",
    deps=["load_meta_series_to_s3"],
)
def load_series_dependencies_to_s3(
    context: AssetExecutionContext, config: LoadSeriesDependenciesConfig
) -> MaterializeResult:
    """Load series dependencies CSV to S3 as Parquet file.

    Args:
        context: Dagster asset execution context
        config: LoadSeriesDependenciesConfig with asset configuration

    Returns:
        MaterializeResult with metadata about the loaded data
    """
    duckdb_resource = context.resources.duckdb
    data_api = DataAPI(duckdb_resource)
    duckdb_repo = DuckDbRepository(duckdb_resource._con)

    data_api.create_temp_table_from_csv(config.csv_path, config.temp_table_name)

    count_query = QueryBuilder(config.temp_table_name)
    count_query.select("COUNT(*) as count")
    count_df = duckdb_repo.fetch_df(count_query)
    row_count = int(count_df.iloc[0]["count"]) if not count_df.empty else 0

    relative_path = build_s3_control_table_path(config.control_table_type)
    all_data_query = QueryBuilder(config.temp_table_name)
    all_data_df = duckdb_repo.fetch_df(all_data_query)
    data_api.save_dataframe_to_s3(all_data_df, relative_path)

    data_api.drop_temp_table(config.temp_table_name)

    context.log.info(
        f"Loaded {row_count} series dependencies rows to S3: {relative_path}",
        extra={
            "row_count": row_count,
            "s3_path": relative_path,
        },
    )

    return MaterializeResult(
        metadata={
            "num_records": row_count,
            "s3_path": relative_path,
        }
    )
