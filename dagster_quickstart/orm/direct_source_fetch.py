"""Direct vendor-source value fetch helpers for QuerySet."""

from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd  # type: ignore[import-untyped]

from dagster_quickstart.orm.exceptions import ValueQueryParameterError
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import (
    MetadataColumns,
    TickerSource,
    ValueColumns,
    get_vendor_ticker_and_field_columns,
)
from dagster_quickstart.orm.ticker_mapping import build_ticker_to_series_map
from dagster_quickstart.utils.datetime_utils import parse_timestamp

LoadMetadataRowsFn = Callable[[Dict[str, List[str]]], pd.DataFrame]


def ticker_and_field_columns(tickersource: TickerSource) -> Tuple[str, str]:
    try:
        return get_vendor_ticker_and_field_columns(tickersource)
    except ValueError as exc:
        raise ValueQueryParameterError(
            f"Direct source fetch not supported for ticker source '{tickersource.value}'"
        ) from exc


def filter_and_sort_direct_value_df(
    df: pd.DataFrame,
    params: Optional[ValueQueryParams],
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.sort_values(ValueColumns.TIMESTAMP)
    if params and params.order_by:
        if params.order_by not in out.columns:
            raise ValueQueryParameterError(
                f"Invalid order_by column '{params.order_by}'. "
                f"Expected one of: {list(out.columns)}"
            )
        out = out.sort_values(params.order_by)
    if params and params.limit is not None:
        out = out.head(int(params.limit))
    return out


def resolve_series_ticker_field_rows(
    load_metadata_rows: LoadMetadataRowsFn,
    series_codes: List[str],
    tickersource: TickerSource,
) -> pd.DataFrame:
    ticker_col, field_col = ticker_and_field_columns(tickersource)
    metadata_df = load_metadata_rows({MetadataColumns.SERIES_CODE: series_codes})
    if metadata_df.empty:
        return pd.DataFrame()
    required_cols = [MetadataColumns.SERIES_CODE, ticker_col, field_col]
    for col in required_cols:
        if col not in metadata_df.columns:
            raise ValueQueryParameterError(
                f"Metadata missing required column '{col}' for source '{tickersource.value}'"
            )
    return metadata_df[required_cols].dropna(subset=[ticker_col, field_col]).copy()


def fetch_direct_bloomberg_tss(
    mapping_df: pd.DataFrame,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    try:
        from pyeqdr.services import tss  # type: ignore
    except Exception as exc:
        raise ValueQueryParameterError(
            "Direct Bloomberg fetch requires pyeqdr services TSS client"
        ) from exc

    if mapping_df.empty:
        return pd.DataFrame(
            columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        )

    frames: List[pd.DataFrame] = []
    _, field_col = ticker_and_field_columns(TickerSource.BLOOMBERG)
    for field_name, group in mapping_df.groupby(field_col):
        ticker_to_series = build_ticker_to_series_map(group, TickerSource.BLOOMBERG)
        symbols = list(ticker_to_series.keys())
        if not symbols:
            continue
        kwargs = {
            "symbols": symbols,
            "flds": [f"bloomberg/ts/{field_name}"],
            "add_live": False,
            "frequency": "B",
        }
        if start_dt is not None:
            kwargs["fromDate"] = start_dt
        if end_dt is not None:
            kwargs["toDate"] = end_dt
        raw_df = tss.get_history(**kwargs)
        if raw_df is None or raw_df.empty:
            continue
        normalized = raw_df.copy()
        if ValueColumns.TIMESTAMP in normalized.columns:
            normalized[ValueColumns.TIMESTAMP] = pd.to_datetime(
                normalized[ValueColumns.TIMESTAMP],
                utc=True,
            )
            normalized = normalized.set_index(ValueColumns.TIMESTAMP)
        elif isinstance(normalized.index, pd.DatetimeIndex):
            normalized.index = pd.to_datetime(normalized.index, utc=True)
        else:
            continue

        for ticker in symbols:
            if ticker not in normalized.columns:
                continue
            series_code = ticker_to_series.get(ticker)
            if not series_code:
                continue
            series_df = normalized[[ticker]].reset_index()
            series_df.columns = [ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            series_df[ValueColumns.SERIES_CODE] = series_code
            frames.append(
                series_df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]]
            )

    if not frames:
        return pd.DataFrame(
            columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        )
    return pd.concat(frames, ignore_index=True)


def fetch_direct_hawk(
    mapping_df: pd.DataFrame,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    try:
        from dagster_quickstart.MQL.base_demo import build_celery_config
        from dagster_quickstart.MQL.hawk import HawkStrategy
    except Exception as exc:
        raise ValueQueryParameterError("Direct Hawk fetch dependencies are unavailable") from exc

    ticker_to_series = build_ticker_to_series_map(mapping_df, TickerSource.HAWKEYE)
    symbols = list(ticker_to_series.keys())
    if not symbols:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    strategy = HawkStrategy(config=build_celery_config())
    result = strategy.get_history(
        symbols=symbols,
        fromDate=start_dt or datetime(1970, 1, 1),
        toDate=end_dt or datetime.utcnow(),
        frequency="D",
    )
    payload = getattr(result, "payload", None)
    if payload is None or payload.empty:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    payload.index = pd.to_datetime(payload.index, utc=True)
    frames: List[pd.DataFrame] = []
    for ticker in symbols:
        if ticker not in payload.columns:
            continue
        sc = ticker_to_series[ticker]
        series_df = payload[[ticker]].reset_index()
        series_df.columns = [ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        series_df[ValueColumns.SERIES_CODE] = sc
        frames.append(series_df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]])
    if not frames:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    return pd.concat(frames, ignore_index=True)


def fetch_direct_onetick(
    mapping_df: pd.DataFrame,
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    try:
        from onetick import OneTick  # type: ignore
    except Exception:
        try:
            from pyeqdr import OneTick  # type: ignore
        except Exception as exc:
            raise ValueQueryParameterError(
                "Direct OneTick fetch requires a OneTick client library"
            ) from exc

    ticker_to_series = build_ticker_to_series_map(mapping_df, TickerSource.MDS)
    symbols = list(ticker_to_series.keys())
    if not symbols:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    client = OneTick()
    raw_df = client.get_history(
        symbols=symbols,
        from_date=start_dt,
        to_date=end_dt,
    )
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    normalized = raw_df.copy()
    if ValueColumns.TIMESTAMP in normalized.columns:
        normalized[ValueColumns.TIMESTAMP] = pd.to_datetime(normalized[ValueColumns.TIMESTAMP], utc=True)
        normalized = normalized.set_index(ValueColumns.TIMESTAMP)
    elif isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index, utc=True)
    else:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    frames: List[pd.DataFrame] = []
    for ticker in symbols:
        if ticker not in normalized.columns:
            continue
        sc = ticker_to_series[ticker]
        series_df = normalized[[ticker]].reset_index()
        series_df.columns = [ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        series_df[ValueColumns.SERIES_CODE] = sc
        frames.append(series_df[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]])
    if not frames:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    return pd.concat(frames, ignore_index=True)


def get_direct_source_values(
    load_metadata_rows: LoadMetadataRowsFn,
    series_codes: List[str],
    tickersource: TickerSource,
    params: Optional[ValueQueryParams],
) -> pd.DataFrame:
    start_dt = parse_timestamp(params.start) if params and params.start else None
    end_dt = parse_timestamp(params.end) if params and params.end else None
    mapping_df = resolve_series_ticker_field_rows(load_metadata_rows, series_codes, tickersource)
    if mapping_df.empty:
        return pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
    if tickersource == TickerSource.BLOOMBERG:
        out = fetch_direct_bloomberg_tss(mapping_df, start_dt, end_dt)
    elif tickersource == TickerSource.HAWKEYE:
        out = fetch_direct_hawk(mapping_df, start_dt, end_dt)
    elif tickersource in (TickerSource.MDS, TickerSource.ONETICK):
        out = fetch_direct_onetick(mapping_df, start_dt, end_dt)
    else:
        raise ValueQueryParameterError(
            f"Direct source fetch is not implemented for '{tickersource.value}'"
        )
    return filter_and_sort_direct_value_df(out, params)
