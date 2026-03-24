"""Bloomberg daily data ingestion asset using PyPDL.

Fetches time-series data from Bloomberg via PyPDL and saves to S3 via DataAPI.
Partitioned by field_type for parallel processing.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
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
from dagster_quickstart.orm.schema import TickerSource, ValueColumns
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


def _transform_layer_long_dataframe(
    tickers: List[str],
    ticker_to_series_code: Dict[str, str],
    data_points: Dict[str, Any],
    request_dates: List[datetime],
) -> Tuple[pd.DataFrame, Dict[str, TickerMergeStats]]:
    """Merge per-ticker API payloads into one long-format frame (timestamp, series_code, value)."""
    rows: List[Dict[str, Any]] = []
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
        merge_stats_by_ticker[ticker] = stats
        for point in merged:
            rows.append(
                {
                    ValueColumns.TIMESTAMP: point["timestamp"],
                    ValueColumns.SERIES_CODE: series_code,
                    ValueColumns.VALUE: point.get("value"),
                }
            )
    if not rows:
        return (
            pd.DataFrame(
                columns=[
                    ValueColumns.TIMESTAMP,
                    ValueColumns.SERIES_CODE,
                    ValueColumns.VALUE,
                ]
            ),
            merge_stats_by_ticker,
        )
    return pd.DataFrame(rows), merge_stats_by_ticker


def _materialize_bloomberg_wide_partition(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    field_type: str,
    series_codes: List[str],
    metadata_series_count: int,
) -> MaterializeResult:
    """Fetch → long → pivot → monthly wide Parquet partitions (shared daily/backfill path)."""
    duckdb_resource = context.resources.duckdb
    pypdl_resource = context.resources.pypdl
    data_api = DataAPI(duckdb_resource)

    start_date = config.get_start_date()
    end_date = config.get_end_date()

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
                "series_count": metadata_series_count,
                "partitions_written": 0,
                "wide_row_count_max": 0,
                "wide_column_count": 0,
                "partition_paths_sample": MetadataValue.json([]),
            }
        )

    if not config.force_refresh:
        data_exists_map = data_api.check_wide_data_exists_for_date_range(
            series_codes=series_codes,
            start_date=start_date,
            end_date=end_date,
            field_type=field_type,
            ticker_source=TickerSource.BLOOMBERG,
        )
        series_codes_to_fetch = [sc for sc in series_codes if not data_exists_map.get(sc, False)]

        if not series_codes_to_fetch:
            context.log.info(
                "All series have wide-partition coverage for date range; skipping PyPDL",
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
                    "series_count": metadata_series_count,
                    "tickers_fetched": 0,
                    "partitions_written": 0,
                    "wide_row_count_max": 0,
                    "wide_column_count": 0,
                    "partition_paths_sample": MetadataValue.json([]),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "skipped": True,
                }
            )

        series_codes = series_codes_to_fetch
        series_code_to_ticker = {
            sc: t for sc, t in series_code_to_ticker.items() if sc in series_codes_to_fetch
        }
        context.log.info(
            f"Wide idempotency: fetching {len(series_codes_to_fetch)} series, "
            f"skipping {len(data_exists_map) - len(series_codes_to_fetch)} with full coverage",
            extra={"field_type": field_type},
        )

    ticker_to_series_code = {v: k for k, v in series_code_to_ticker.items()}
    tickers = list(ticker_to_series_code.keys())

    context.log.info(
        f"Fetching data for {len(tickers)} tickers",
        extra={"field_type": field_type, "ticker_count": len(tickers)},
    )

    data_source, _, _, _ = build_pypdl_request_params(
        field_name=field_type,
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
    )

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
            "PyPDL fetch failed; persisting null-filled rows for requested range",
            extra={
                "field_type": field_type,
                "error_reason": error_reason,
                "ticker_count": len(tickers),
            },
        )

    if data_points is None:
        data_points = {}

    request_dates = _dates_inclusive_utc(start_date, end_date)
    long_df, merge_stats_by_ticker = _transform_layer_long_dataframe(
        tickers=tickers,
        ticker_to_series_code=ticker_to_series_code,
        data_points=data_points,
        request_dates=request_dates,
    )

    invalid_details_full = _build_ingestion_invalid_details(
        tickers=tickers,
        ticker_to_series_code=ticker_to_series_code,
        error_reason=error_reason,
        merge_stats_by_ticker=merge_stats_by_ticker,
    )
    invalid_count = len(invalid_details_full)
    invalid_series_codes_sorted = sorted({r["series_code"] for r in invalid_details_full})

    write_stats = data_api.save_bloomberg_wide_from_long(
        long_df=long_df,
        field_type=field_type,
        ticker_source=TickerSource.BLOOMBERG,
        start_date=start_date,
        end_date=end_date,
        force_refresh=config.force_refresh,
    )

    context.log.info(
        "Wide partition write complete",
        extra={
            "field_type": field_type,
            "partitions_written": write_stats["partitions_written"],
            "wide_row_count_max": write_stats["row_count_max"],
            "wide_column_count": write_stats["column_count"],
        },
    )

    max_paths = 20
    paths = write_stats["written_relative_paths"][:max_paths]

    result_metadata: Dict[str, Any] = {
        "field_type": field_type,
        "series_count": metadata_series_count,
        "tickers_fetched": len(tickers),
        "partitions_written": write_stats["partitions_written"],
        "wide_row_count_max": write_stats["row_count_max"],
        "wide_column_count": write_stats["column_count"],
        "data_points_saved": int(len(long_df)),
        "partition_paths_sample": MetadataValue.json(paths),
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
    """Daily ingestion: PyPDL fetch, pivot to wide time-by-series matrix, monthly Parquet partitions.

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

    metadata_series_count = len(series_codes)
    return _materialize_bloomberg_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count,
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

    return _materialize_bloomberg_wide_partition(
        context,
        config,
        field_type,
        series_codes,
        metadata_series_count=len(series_codes),
    )
