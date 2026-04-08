"""Hawk (Hawkeye) ingestion Dagster assets — daily and backfill on ``vol`` partition."""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.ingestion import HAWK_FIELD_TYPE_PARTITIONS
from dagster_quickstart.assets.ingestion.bloomberg.config import IngestionMode
from dagster_quickstart.assets.ingestion.hawk.config import HawkIngestionConfig
from dagster_quickstart.assets.ingestion.hawk.wide_materialize import materialize_hawk_wide_partition
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.schema import TickerSource


@asset(
    partitions_def=HAWK_FIELD_TYPE_PARTITIONS,
    required_resource_keys={"duckdb", "hawk"},
    name="ingest_hawk_data_daily",
)
def ingest_hawk_data_daily(
    context: AssetExecutionContext,
    config: HawkIngestionConfig,
) -> MaterializeResult:
    """Daily Hawk ingestion: metadata-driven fame codes, wide monthly Parquet under Hawkeye."""
    if config.mode != IngestionMode.DAILY:
        raise ValueError(
            f"ingest_hawk_data_daily requires mode=IngestionMode.DAILY, got {config.mode}"
        )

    field_type = context.partition_key
    start_date = config.get_start_date()
    end_date = config.get_end_date()

    context.log.info(
        f"Starting daily Hawk wide ingestion for field_type={field_type}",
        extra={
            "field_type": field_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    data_api = DataAPI(context.resources.duckdb)
    series_codes = data_api.get_series_codes(
        field_type=field_type,
        ticker_source=TickerSource.HAWKEYE,
    )

    if not series_codes:
        context.log.warning(
            f"No series codes found for hawk_field={field_type}",
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
        f"Found {len(series_codes)} series codes for hawk_field={field_type}",
        extra={"field_type": field_type, "series_count": len(series_codes)},
    )

    return materialize_hawk_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count=len(series_codes),
    )


@asset(
    partitions_def=HAWK_FIELD_TYPE_PARTITIONS,
    required_resource_keys={"duckdb", "hawk"},
    name="ingest_hawk_data_backfill",

)
def ingest_hawk_data_backfill(
    context: AssetExecutionContext,
    config: HawkIngestionConfig,
) -> MaterializeResult:
    """Backfill selected ``series_codes`` into the same wide monthly partitions as daily."""
    if config.mode != IngestionMode.BACKFILL:
        raise ValueError(
            f"ingest_hawk_data_backfill requires mode=IngestionMode.BACKFILL, got {config.mode}"
        )
    if not config.series_codes:
        raise ValueError("ingest_hawk_data_backfill requires non-empty config.series_codes")

    field_type = context.partition_key
    start_date = config.get_start_date()
    end_date = config.get_end_date()

    context.log.info(
        f"Starting Hawk wide backfill for field_type={field_type}",
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
            ticker_source=TickerSource.HAWKEYE,
        )
    )
    series_codes = [sc for sc in config.series_codes if sc in allowed]

    if not series_codes:
        context.log.warning(
            "No requested series_codes match metadata for this hawk_field partition",
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

    return materialize_hawk_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count=len(series_codes),
    )
