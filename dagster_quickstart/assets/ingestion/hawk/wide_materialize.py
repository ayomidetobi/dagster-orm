"""Hawk wide ingestion: MQL history pull, align to calendar grid, write monthly partitions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from dagster_quickstart.assets.ingestion.hawk.config import HawkIngestionConfig
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.schema import TickerSource, ValueColumns
from dagster_quickstart.utils.datetime_utils import utc_calendar_days_inclusive, utc_midnight


def _hawk_payload_to_wide(
    payload: Any,
    request_dates: List[datetime],
    series_codes: List[str],
    fame_to_series_code: Dict[str, str],
) -> pd.DataFrame:
    """Map Hawk ``DataResult.raw.payload`` (wide DataFrame) to series_code columns on ``request_dates``."""
    idx = pd.DatetimeIndex(
        [pd.Timestamp(utc_midnight(d)) for d in request_dates],
        name=ValueColumns.TIMESTAMP,
    )
    empty = pd.DataFrame(index=idx, columns=list(series_codes), dtype="float64")
    empty.columns.name = None
    if payload is None or not isinstance(payload, pd.DataFrame) or payload.empty:
        return empty

    df = payload.copy()
    if ValueColumns.TIMESTAMP in df.columns:
        df = df.set_index(ValueColumns.TIMESTAMP)
    df.index = pd.DatetimeIndex([utc_midnight(pd.Timestamp(ts).to_pydatetime()) for ts in df.index])
    df = df.sort_index()
    rename = {k: v for k, v in fame_to_series_code.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [c for c in series_codes if c in df.columns]
    if keep:
        df = df[keep]
    else:
        df = pd.DataFrame(index=df.index)
    df = df.reindex(idx)
    for sc in series_codes:
        if sc not in df.columns:
            df[sc] = float("nan")
    out = df[series_codes].copy()
    out.index.name = ValueColumns.TIMESTAMP
    return out.sort_index()


def materialize_hawk_wide_partition(
    context: AssetExecutionContext,
    config: HawkIngestionConfig,
    field_type: str,
    series_codes: List[str],
    metadata_series_count: int,
) -> MaterializeResult:
    """Fetch Hawk history → wide matrix → monthly Parquet (shared daily/backfill path)."""
    duckdb_resource = context.resources.duckdb
    hawk_resource = context.resources.hawk
    data_api = DataAPI(duckdb_resource)

    start_date = config.get_start_date()
    end_date = config.get_end_date()

    series_code_to_fame = data_api.get_tickers(
        series_codes=series_codes,
        field_type=field_type,
        ticker_source=TickerSource.HAWKEYE,
    )

    if not series_code_to_fame:
        context.log.warning(
            f"No Hawk fame codes for {len(series_codes)} series codes",
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
            ticker_source=TickerSource.HAWKEYE,
        )
        series_codes_to_fetch = [sc for sc in series_codes if not data_exists_map.get(sc, False)]
        if not series_codes_to_fetch:
            context.log.info(
                "All series have wide-partition coverage for date range; skipping Hawk fetch",
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
        series_code_to_fame = {
            sc: fc for sc, fc in series_code_to_fame.items() if sc in series_codes_to_fetch
        }

    fame_to_series_code = {v: k for k, v in series_code_to_fame.items()}
    fame_codes = list(fame_to_series_code.keys())

    context.log.info(
        f"Hawk fetch for {len(fame_codes)} fame codes",
        extra={"field_type": field_type, "fame_count": len(fame_codes)},
    )

    error_reason: Optional[str] = None
    payload: Any = None
    try:
        result = hawk_resource.fetch_history(
            symbols=fame_codes,
            start=start_date,
            end=end_date,
        )
        payload = result.raw.payload
    except Exception as exc:
        error_reason = str(exc)
        context.log.warning(
            "Hawk fetch failed; writing null-filled wide rows for range",
            extra={"field_type": field_type, "error_reason": error_reason},
        )

    request_dates = utc_calendar_days_inclusive(start_date, end_date)
    wide_df = _hawk_payload_to_wide(
        payload,
        request_dates,
        series_codes,
        fame_to_series_code,
    )

    write_stats = data_api.write_wide_value_partitions(
        wide_df=wide_df,
        field_type=field_type,
        ticker_source=TickerSource.HAWKEYE,
        start_date=start_date,
        end_date=end_date,
        force_refresh=config.force_refresh,
    )

    context.log.info(
        "Hawk wide partition write complete",
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
        "fame_codes_fetched": len(fame_codes),
        "partitions_written": write_stats["partitions_written"],
        "wide_row_count_max": write_stats["row_count_max"],
        "wide_column_count": write_stats["column_count"],
        "data_points_saved": int(wide_df.shape[0] * wide_df.shape[1]),
        "partition_paths_sample": MetadataValue.json(paths),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    if error_reason:
        result_metadata["hawk_error_reason"] = error_reason[:500]

    return MaterializeResult(metadata=result_metadata)
