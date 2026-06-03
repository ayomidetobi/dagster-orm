"""Bloomberg wide ingestion: DataAPI value fetch, merge to wide frame, write monthly partitions."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

from dagster_quickstart.assets.ingestion.bloomberg.config import BloombergIngestionConfig
from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.exceptions import ValueQueryParameterError
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, TickerSource, ValueColumns
from dagster_quickstart.orm.schema.constants import (
    MAX_INVALID_METADATA_ROWS,
    MAX_INVALID_VALUE_CHARS,
)
from dagster_quickstart.orm.ticker_mapping import build_series_to_ticker_map
from dagster_quickstart.utils.datetime_utils import utc_calendar_days_inclusive


def _date_param(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def truncate_invalid_value(text: str, max_chars: int = MAX_INVALID_VALUE_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars - 3]}..."


def invalid_detail_row(
    series_code: str, ticker: str, invalid_column: str, invalid_value: str
) -> Dict[str, str]:
    """Same shape as validate_metadata_against_lookup invalid rows, plus ticker."""
    return {
        "series_code": series_code,
        "ticker": ticker,
        "invalid_column": invalid_column,
        "invalid_value": invalid_value,
    }


def build_ingestion_invalid_details_from_wide(
    wide_df: pd.DataFrame,
    series_code_to_ticker: Dict[str, str],
    request_dates: List[datetime],
    error_reason: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Build invalid_details rows from a wide value frame and the requested calendar days."""
    rows: List[Dict[str, str]] = []
    ev = truncate_invalid_value(error_reason) if error_reason else ""

    if error_reason:
        for series_code, ticker in series_code_to_ticker.items():
            rows.append(invalid_detail_row(series_code, ticker, "value_fetch", ev))
        return rows

    total_days = len(request_dates)
    for series_code, ticker in series_code_to_ticker.items():
        if series_code not in wide_df.columns:
            rows.append(invalid_detail_row(series_code, ticker, "value_series", "no_data_returned"))
            continue
        col = wide_df[series_code]
        raw_count = int(col.notna().sum())
        if raw_count == 0:
            rows.append(invalid_detail_row(series_code, ticker, "value_series", "no_data_returned"))
            continue
        null_filled = total_days - raw_count
        if null_filled > 0:
            msg = f"{null_filled}/{total_days} days null or missing in range"
            rows.append(invalid_detail_row(series_code, ticker, "value", msg))

    return rows


def fetch_bloomberg_values_wide(
    data_api: DataAPI,
    series_codes: List[str],
    start_date: datetime,
    end_date: datetime,
    *,
    ticker_source: TickerSource,
    out_of_cache: bool,
) -> pd.DataFrame:
    """Load values for ``series_codes`` via :meth:`QuerySet.value` for the given vendor."""
    queryset = data_api.get(
        control_table=TableNames.METADATA,
        series_code=series_codes,
    )
    return queryset.value(
        params=ValueQueryParams(
            start=_date_param(start_date),
            end=_date_param(end_date),
        ),
        tickersource=ticker_source,
        out_of_cache=out_of_cache,
    )


def materialize_bloomberg_wide_partition(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    field_type: str,
    metadata_df: pd.DataFrame,
) -> MaterializeResult:
    """Fetch via DataAPI → wide matrix → monthly wide Parquet partitions (daily/backfill)."""
    data_api = context.resources.data_api.get_api()
    ticker_source = TickerSource.BLOOMBERG

    start_date = config.get_start_date()
    end_date = config.get_end_date()

    metadata_series_count = len(metadata_df)
    series_codes = (
        metadata_df[MetadataColumns.SERIES_CODE].dropna().astype(str).unique().tolist()
    )

    series_code_to_ticker = build_series_to_ticker_map(metadata_df, ticker_source)

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
            ticker_source=ticker_source,
        )
        series_codes_to_fetch = [sc for sc in series_codes if not data_exists_map.get(sc, False)]

        if not series_codes_to_fetch:
            context.log.info(
                "All series have wide-partition coverage for date range; skipping value fetch",
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
                    "series_fetched": 0,
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

    context.log.info(
        f"Fetching values for {len(series_codes)} series via DataAPI",
        extra={
            "field_type": field_type,
            "series_count": len(series_codes),
            "ticker_source": ticker_source.value,
        },
    )

    error_reason: Optional[str] = None
    wide_df = pd.DataFrame()
    try:
        wide_df = fetch_bloomberg_values_wide(
            data_api,
            series_codes,
            start_date,
            end_date,
            ticker_source=ticker_source,
            out_of_cache=True,
        )
    except ValueQueryParameterError as exc:
        error_reason = str(exc)
        context.log.warning(
            "DataAPI value fetch failed",
            extra={"field_type": field_type, "error_reason": error_reason},
        )
    except Exception as exc:
        error_reason = str(exc)
        context.log.warning(
            "DataAPI value fetch failed",
            extra={"field_type": field_type, "error_reason": error_reason},
            exc_info=True,
        )

    if wide_df.empty and not error_reason:
        context.log.warning(
            "No value rows returned from DataAPI for requested series and date range",
            extra={"field_type": field_type, "series_count": len(series_codes)},
        )

    if wide_df.index.name != ValueColumns.TIMESTAMP:
        wide_df.index.name = ValueColumns.TIMESTAMP

    request_dates = utc_calendar_days_inclusive(start_date, end_date)
    invalid_details_full = build_ingestion_invalid_details_from_wide(
        wide_df=wide_df,
        series_code_to_ticker=series_code_to_ticker,
        request_dates=request_dates,
        error_reason=error_reason,
    )
    invalid_count = len(invalid_details_full)
    invalid_series_codes_sorted = sorted({r["series_code"] for r in invalid_details_full})

    write_stats = data_api.write_wide_value_partitions(
        wide_df=wide_df,
        field_type=field_type,
        ticker_source=ticker_source,
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
        "series_fetched": len(series_codes),
        "ticker_source": ticker_source.value,
        "out_of_cache": True,
        "partitions_written": write_stats["partitions_written"],
        "wide_row_count_max": write_stats["row_count_max"],
        "wide_column_count": write_stats["column_count"],
        "data_points_saved": int(wide_df.shape[0] * wide_df.shape[1]) if not wide_df.empty else 0,
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
        result_metadata["value_fetch_error_reason"] = truncate_invalid_value(error_reason)

    return MaterializeResult(metadata=result_metadata)
