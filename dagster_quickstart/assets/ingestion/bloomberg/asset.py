"""Bloomberg daily data ingestion asset using PyPDL.

Fetches time-series data from Bloomberg via PyPDL and saves to S3 via DataAPI.
Partitioned by field_type for parallel processing.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
from dagster_quickstart.utils.datetime_utils import ensure_utc
from dagster_quickstart.utils.pypdl_helpers import (
    build_pypdl_request_params,
    fetch_bloomberg_data,
)

# Match load_metaseries / load_series_dependencies checks: cap rows in run metadata
MAX_INVALID_METADATA_ROWS = 20
MAX_INVALID_VALUE_CHARS = 500


@dataclass(frozen=True)
class TickerMergeStats:
    """Per-ticker merge outcome for building invalid_details metadata."""

    raw_api_point_count: int
    null_filled_day_count: int
    total_days: int


def _dates_inclusive_utc(start_date: datetime, end_date: datetime) -> List[datetime]:
    """UTC midnight for each calendar day from start through end (inclusive)."""
    start = ensure_utc(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
    end = ensure_utc(end_date).replace(hour=0, minute=0, second=0, microsecond=0)
    dates: List[datetime] = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _utc_midnight(dt: datetime) -> datetime:
    return ensure_utc(dt).replace(hour=0, minute=0, second=0, microsecond=0)


def _merge_ticker_points_with_null_fill(
    raw_points: Optional[List[Dict[str, Any]]],
    dates: List[datetime],
) -> Tuple[List[Dict[str, Any]], TickerMergeStats]:
    """One row per day in ``dates``; use API value when present, else None.

    Missing days, None values, and invalid/non-finite numbers use Python ``None``;
    Parquet float columns persist those as null (typically read back as NaN).
    If the same day appears more than once, the last wins.

    Returns:
        Merged points and stats for Dagster metadata (invalid_details).
    """
    raw_list = raw_points or []
    raw_api_point_count = len(raw_list)

    value_by_day: Dict[datetime, Optional[float]] = {}
    for point in raw_list:
        day = _utc_midnight(point["timestamp"])
        val = point.get("value")
        if val is None:
            value_by_day[day] = None
        else:
            try:
                fv = float(val)
                value_by_day[day] = fv if fv == fv else None
            except (TypeError, ValueError):
                value_by_day[day] = None

    merged: List[Dict[str, Any]] = []
    null_filled_day_count = 0
    for d in dates:
        d0 = _utc_midnight(d)
        v = value_by_day.get(d0)
        if v is None:
            null_filled_day_count += 1
        merged.append({"timestamp": d0, "value": v})

    stats = TickerMergeStats(
        raw_api_point_count=raw_api_point_count,
        null_filled_day_count=null_filled_day_count,
        total_days=len(dates),
    )
    return merged, stats


def _truncate_invalid_value(text: str, max_chars: int = MAX_INVALID_VALUE_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars - 3]}..."


def _invalid_detail_row(
    series_code: str, ticker: str, invalid_column: str, invalid_value: str
) -> Dict[str, str]:
    """Same shape as validate_metadata_against_lookup invalid rows, plus ticker."""
    return {
        "series_code": series_code,
        "ticker": ticker,
        "invalid_column": invalid_column,
        "invalid_value": invalid_value,
    }


def _build_ingestion_invalid_details(
    tickers: List[str],
    ticker_to_series_code: Dict[str, str],
    error_reason: Optional[str],
    merge_stats_by_ticker: Dict[str, TickerMergeStats],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    ev = _truncate_invalid_value(error_reason) if error_reason else ""

    if error_reason:
        for ticker in tickers:
            sc = ticker_to_series_code.get(ticker)
            if sc:
                rows.append(_invalid_detail_row(sc, ticker, "pypdl_fetch", ev))
        return rows

    for ticker in tickers:
        sc = ticker_to_series_code.get(ticker)
        stats = merge_stats_by_ticker.get(ticker) if sc else None
        if not sc or stats is None:
            continue
        if stats.raw_api_point_count == 0:
            rows.append(_invalid_detail_row(sc, ticker, "pypdl_series", "no_data_returned"))
        elif stats.null_filled_day_count > 0:
            msg = f"{stats.null_filled_day_count}/{stats.total_days} days null or invalid"
            rows.append(_invalid_detail_row(sc, ticker, "value", msg))

    return rows


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
        context.log.warning(
            "PyPDL fetch failed; persisting null values for full requested date range per series",
            extra={
                "field_type": field_type,
                "error_reason": error_reason,
                "ticker_count": len(tickers),
            },
        )

    if data_points is None:
        data_points = {}

    request_dates = _dates_inclusive_utc(start_date, end_date)

    # Convert ticker-keyed results to series_code keys; fill missing tickers/days with None
    data_points_by_series_code: Dict[str, List[Dict[str, Any]]] = {}
    merge_stats_by_ticker: Dict[str, TickerMergeStats] = {}
    for ticker in tickers:
        series_code = ticker_to_series_code.get(ticker)
        if not series_code:
            continue
        raw = data_points.get(ticker)
        merged, stats = _merge_ticker_points_with_null_fill(
            raw if isinstance(raw, list) else None,
            request_dates,
        )
        data_points_by_series_code[series_code] = merged
        merge_stats_by_ticker[ticker] = stats

    invalid_details_full = _build_ingestion_invalid_details(
        tickers=tickers,
        ticker_to_series_code=ticker_to_series_code,
        error_reason=error_reason,
        merge_stats_by_ticker=merge_stats_by_ticker,
    )
    invalid_count = len(invalid_details_full)
    invalid_series_codes_sorted = sorted({r["series_code"] for r in invalid_details_full})

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

    # Limit number of S3 paths reported in metadata to avoid overly large payloads
    max_s3_paths = 20
    s3_paths_list = list(saved_paths.values())
    limited_s3_paths = s3_paths_list[:max_s3_paths]

    result_metadata: Dict[str, Any] = {
        "field_type": field_type,
        "series_count": original_series_count,
        "tickers_fetched": len(tickers),
        "series_saved": len(saved_paths),
        "data_points_saved": total_data_points,
        "s3_paths": MetadataValue.json(limited_s3_paths),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "invalid_count": invalid_count,
    }
    if invalid_count > 0:
        result_metadata["invalid_details"] = MetadataValue.json(
            invalid_details_full[:MAX_INVALID_METADATA_ROWS]
        )
        result_metadata["invalid_series_codes"] = MetadataValue.json(invalid_series_codes_sorted)
    if error_reason:
        result_metadata["pypdl_error_reason"] = _truncate_invalid_value(error_reason)

    return MaterializeResult(metadata=result_metadata)


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
