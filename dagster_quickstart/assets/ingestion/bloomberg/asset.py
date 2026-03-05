"""Bloomberg daily data ingestion asset using PyPDL.

Fetches time-series data from Bloomberg via PyPDL and saves to S3 via DataAPI.
Partitioned by field_type for parallel processing.
"""

from typing import Any, Dict, List

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from dagster_quickstart.assets.ingestion.bloomberg.config import (
    BloombergIngestionConfig,
    IngestionMode,
)
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.schema import TickerSource
from dagster_quickstart.utils.pypdl_helpers import (
    build_pypdl_request_params,
    fetch_bloomberg_data,
)

# Define field_type partitions
FIELD_TYPE_PARTITIONS = StaticPartitionsDefinition(
    [
        "PX_LAST",
        "PX_OPEN",
        "PX_HIGH",
        "PX_LOW",
        "PX_VOLUME",
        "YIELD_CURVE",
        "SPREAD",
        "RATE",
    ]
)


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
    """Daily ingestion asset for Bloomberg data via PyPDL.

    Fetches time-series data for all series matching the partition's field_type
    and saves to S3. Uses series_code from partition or fetches from metadata.

    Args:
        context: Dagster asset execution context
        config: BloombergIngestionConfig with ingestion settings

    Returns:
        MaterializeResult with ingestion metadata for this partition
    """
    # Ensure mode is DAILY
    if config.mode != IngestionMode.DAILY:
        raise ValueError(
            f"ingest_bloomberg_data_daily requires mode=IngestionMode.DAILY, got {config.mode}"
        )

    duckdb_resource = context.resources.duckdb
    pypdl_resource = context.resources.pypdl
    data_api = DataAPI(duckdb_resource)

    # Get field_type from partition
    field_type = context.partition_key

    # Get date range from config (defaults to today)
    start_date = config.get_start_date()
    end_date = config.get_end_date()

    context.log.info(
        f"Starting daily ingestion for field_type={field_type}",
        extra={
            "field_type": field_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "use_dummy_data": config.use_dummy_data,
        },
    )

    # Get series codes for this field_type
    series_codes = data_api.get_series_codes(
        field_type=field_type,
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
                "data_points_saved": 0,
                "s3_paths": MetadataValue.json([]),
            }
        )

    context.log.info(
        f"Found {len(series_codes)} series codes for field_type={field_type}",
        extra={"field_type": field_type, "series_count": len(series_codes)},
    )

    # Get ticker mapping for all series codes (returns series_code -> ticker)
    series_code_to_ticker = data_api.get_tickers(
        series_codes=series_codes,
        field_type=field_type,
        ticker_source=TickerSource.BLOOMBERG,
    )

    if not series_code_to_ticker:
        context.log.warning(
            f"No tickers found for {len(series_codes)} series codes",
            extra={"field_type": field_type, "series_count": len(series_codes)},
        )
        return MaterializeResult(
            metadata={
                "field_type": field_type,
                "series_count": len(series_codes),
                "data_points_saved": 0,
                "s3_paths": MetadataValue.json([]),
            }
        )

    # Track original series count for metadata
    original_series_count = len(series_codes)

    # Check if data already exists for idempotency (skip PyPDL query if force_refresh=False)
    if not config.force_refresh:
        data_exists_map = data_api.check_data_exists_for_date_range(
            series_codes=series_codes,
            start_date=start_date,
            end_date=end_date,
            ticker_source=TickerSource.BLOOMBERG,
        )

        # Filter out series codes that already have data
        series_codes_to_fetch = [sc for sc in series_codes if not data_exists_map.get(sc, False)]

        if not series_codes_to_fetch:
            context.log.info(
                f"All {len(series_codes)} series already have data for date range, skipping PyPDL query",
                extra={
                    "field_type": field_type,
                    "series_count": len(series_codes),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
            return MaterializeResult(
                metadata={
                    "field_type": field_type,
                    "series_count": len(series_codes),
                    "tickers_fetched": 0,
                    "series_saved": 0,
                    "data_points_saved": 0,
                    "s3_paths": MetadataValue.json([]),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "skipped": True,
                }
            )

        # Update series_codes and ticker mappings to only include those that need fetching
        series_codes = series_codes_to_fetch
        series_code_to_ticker = {
            sc: ticker
            for sc, ticker in series_code_to_ticker.items()
            if sc in series_codes_to_fetch
        }

        context.log.info(
            f"Skipping {len(data_exists_map) - len(series_codes_to_fetch)} series with existing data, "
            f"fetching {len(series_codes_to_fetch)} series",
            extra={
                "field_type": field_type,
                "series_to_fetch": len(series_codes_to_fetch),
                "series_skipped": len(data_exists_map) - len(series_codes_to_fetch),
            },
        )

    # Reverse mapping for save_value_data_to_s3 (needs ticker -> series_code)
    ticker_to_series_code = {v: k for k, v in series_code_to_ticker.items()}
    tickers = list(ticker_to_series_code.keys())

    context.log.info(
        f"Fetching data for {len(tickers)} tickers",
        extra={"field_type": field_type, "ticker_count": len(tickers)},
    )

    # Build PyPDL request parameters
    data_source, _, _, _ = build_pypdl_request_params(
        field_name=field_type,
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
    )

    # Fetch data from PyPDL
    data_points, error_reason = fetch_bloomberg_data(
        pypdl_resource=pypdl_resource,
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        series_codes=series_codes,
        context=context,
        data_codes=tickers,
        use_dummy_data=config.use_dummy_data,
    )

    if error_reason:
        raise RuntimeError(f"PyPDL fetch failed: {error_reason}")

    if data_points is None:
        raise RuntimeError("PyPDL fetch returned None data_points")

    # Convert data_points from ticker keys to series_code keys
    # fetch_bloomberg_data returns Dict[ticker, List[DataPoint]]
    # but save_value_data_to_s3 expects Dict[series_code, List[Dict[str, Any]]]
    data_points_by_series_code: Dict[str, List[Dict[str, Any]]] = {}
    for ticker, points in data_points.items():
        series_code = ticker_to_series_code.get(ticker)
        if series_code:
            # Convert DataPoint TypedDicts to regular dicts for type compatibility
            data_points_by_series_code[series_code] = [
                {"timestamp": point["timestamp"], "value": point["value"]} for point in points
            ]

    # Save data to S3 using series_code keys
    saved_paths = data_api.save_value_data_to_s3(
        data_points=data_points_by_series_code,
        ticker_source=TickerSource.BLOOMBERG,
        force_refresh=config.force_refresh,
        start_date=start_date,
        end_date=end_date,
    )

    # Calculate total data points saved
    total_data_points = sum(len(points) for points in data_points_by_series_code.values())

    context.log.info(
        f"Saved data for {len(saved_paths)} series to S3",
        extra={
            "field_type": field_type,
            "series_saved": len(saved_paths),
            "total_data_points": total_data_points,
        },
    )

    return MaterializeResult(
        metadata={
            "field_type": field_type,
            "series_count": original_series_count,
            "tickers_fetched": len(tickers),
            "series_saved": len(saved_paths),
            "data_points_saved": total_data_points,
            "s3_paths": MetadataValue.json(list(saved_paths.values())),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
    )


@asset(
    partitions_def=FIELD_TYPE_PARTITIONS,
    required_resource_keys={"duckdb", "pypdl"},
)
def ingest_bloomberg_data_backfill(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
) -> MaterializeResult:
    """Daily ingestion asset for Bloomberg data via PyPDL.

    Fetches time-series data for all series matching the partition's field_type
    and saves to S3 via DataAPI. Partitioned by field_type for parallel processing.
    """
    pass  # TODO: Implement this
