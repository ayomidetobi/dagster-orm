"""Bloomberg ingestion Dagster assets (definitions only)."""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.ingestion.bloomberg.config import (
    BloombergIngestionConfig,
    IngestionMode,
)
from dagster_quickstart.assets.ingestion import FIELD_TYPE_PARTITIONS
from dagster_quickstart.assets.ingestion.bloomberg.wide_materialize import (
    materialize_bloomberg_wide_partition,
)
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.schema import TickerSource


@asset(
    partitions_def=FIELD_TYPE_PARTITIONS,
    required_resource_keys={"duckdb", "pypdl"},
    name="ingest_bloomberg_data_daily",
    deps=["load_lookup_tables_to_s3", "load_meta_series_to_s3", "load_series_dependencies_to_s3"],
)
def ingest_bloomberg_data_daily(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
) -> MaterializeResult:
    """Daily ingestion: PyPDL fetch, wide time-by-series matrix, monthly Parquet partitions.

    Storage layout: ``value-data/wide/{source}/field_type={ft}/year=YYYY/month=MM/data.parquet``.

    Args:
        context: Dagster asset execution context
        config: BloombergIngestionConfig with ingestion settings

    Returns:
        MaterializeResult with partitions_written, row/column counts, and sample paths
    """
    if config.mode != IngestionMode.DAILY:
        raise ValueError(
            f"ingest_bloomberg_data_daily requires mode=IngestionMode.DAILY, got {config.mode}"
        )

    field_type = context.partition_key
    start_date = config.get_start_date()
    end_date = config.get_end_date()

    context.log.info(
        f"Starting daily wide ingestion for field_type={field_type}",
        extra={
            "field_type": field_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "use_dummy_data": config.use_dummy_data,
        },
    )

    data_api = DataAPI(context.resources.duckdb)
    series_codes = data_api.get_series_codes(
        field_type=field_type,
        ticker_source=TickerSource.BLOOMBERG,
    )

    if not series_codes:
        context.log.warning(
            f"No series codes found for field_type={field_type}",
            extra={"field_type": field_type},
        )
        return MaterializeResult(
            metadata={
                "field_type": field_type,
                "series_count": 0,
                "partitions_written": 0,
                "wide_row_count_max": 0,
                "wide_column_count": 0,
                "data_points_saved": 0,
                "partition_paths_sample": MetadataValue.json([]),
            }
        )

    context.log.info(
        f"Found {len(series_codes)} series codes for field_type={field_type}",
        extra={"field_type": field_type, "series_count": len(series_codes)},
    )

    return materialize_bloomberg_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count=len(series_codes),
    )


@asset(
    partitions_def=FIELD_TYPE_PARTITIONS,
    required_resource_keys={"duckdb", "pypdl"},
    deps=["load_lookup_tables_to_s3", "load_meta_series_to_s3", "load_series_dependencies_to_s3"],
)
def ingest_bloomberg_data_backfill(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
) -> MaterializeResult:
    """Backfill selected ``series_codes`` into the same wide monthly partitions as daily ingestion.

    Only series present in metadata for the partition ``field_type`` are processed; others are
    ignored. Merging updates only the affected columns and timestamps; other series in the
    partition are preserved.
    """
    if config.mode != IngestionMode.BACKFILL:
        raise ValueError(
            f"ingest_bloomberg_data_backfill requires mode=IngestionMode.BACKFILL, got {config.mode}"
        )
    if not config.series_codes:
        raise ValueError("ingest_bloomberg_data_backfill requires non-empty config.series_codes")

    field_type = context.partition_key
    start_date = config.get_start_date()
    end_date = config.get_end_date()

    context.log.info(
        f"Starting Bloomberg wide backfill for field_type={field_type}",
        extra={
            "field_type": field_type,
            "requested_series": len(config.series_codes),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    data_api = DataAPI(context.resources.duckdb)
    allowed = set(
        data_api.get_series_codes(
            field_type=field_type,
            ticker_source=TickerSource.BLOOMBERG,
        )
    )
    series_codes = [sc for sc in config.series_codes if sc in allowed]

    if not series_codes:
        context.log.warning(
            "No requested series_codes match metadata for this field_type partition",
            extra={"field_type": field_type},
        )
        return MaterializeResult(
            metadata={
                "field_type": field_type,
                "series_count": 0,
                "partitions_written": 0,
                "wide_row_count_max": 0,
                "wide_column_count": 0,
                "partition_paths_sample": MetadataValue.json([]),
            }
        )

    return materialize_bloomberg_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count=len(series_codes),
    )
