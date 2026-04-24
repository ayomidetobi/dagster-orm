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
from dagster_quickstart.orm.ticker_mapping import build_series_to_ticker_map
from dagster_quickstart.utils.datetime_utils import parse_timestamp

LoadMetadataRowsFn = Callable[[Dict[str, List[str]]], pd.DataFrame]


def empty_direct_value_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            ValueColumns.SERIES_CODE,
            ValueColumns.TIMESTAMP,
            ValueColumns.VALUE,
        ]
    )


def empty_direct_source_raw_df() -> pd.DataFrame:
    return pd.DataFrame()


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


def reshape_direct_source_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty or not isinstance(raw_df.index, pd.DatetimeIndex):
        return empty_direct_value_df()

    normalized = raw_df.copy()
    normalized.index.names = [ValueColumns.TIMESTAMP]
    out = (
        normalized.reset_index()
        .melt(
            id_vars=[ValueColumns.TIMESTAMP],
            var_name=ValueColumns.SERIES_CODE,
            value_name=ValueColumns.VALUE,
        )
        .dropna(subset=[ValueColumns.VALUE])
    )
    return out[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]]


def fetch_direct_bloomberg_tss(
    ticker_fields: Dict[str, Dict[str, str]],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    """
    Fetch raw business-day data from Bloomberg TSS.
    """
    try:
        from pyeqdr.services import tss  # type: ignore
    except Exception as exc:
        raise ValueQueryParameterError(
            "Direct Bloomberg fetch requires pyeqdr services TSS client"
        ) from exc

    if not ticker_fields:
        return empty_direct_source_raw_df()

    frames: List[pd.DataFrame] = []
    for field_name, tickers in ticker_fields.items():
        symbols = list(tickers.values())
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

        field_df = raw_df.copy()
        if ValueColumns.TIMESTAMP in field_df.columns:
            field_df[ValueColumns.TIMESTAMP] = pd.to_datetime(
                field_df[ValueColumns.TIMESTAMP],
                utc=True,
            )
            field_df = field_df.set_index(ValueColumns.TIMESTAMP)
        elif isinstance(field_df.index, pd.DatetimeIndex):
            field_df.index = pd.to_datetime(field_df.index, utc=True)
        else:
            continue

        rename_map = {
            ticker: series_code
            for series_code, ticker in tickers.items()
            if ticker in field_df.columns
        }
        if not rename_map:
            continue

        field_df = field_df.rename(columns=rename_map)
        frames.append(field_df[list(rename_map.values())])

    if not frames:
        return empty_direct_source_raw_df()
    return pd.concat(frames, axis=1)


def fetch_direct_hawk(
    tickers: Dict[str, str],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    """
    Fetch raw daily data from Hawk.
    """
    try:
        from dagster_quickstart.MQL.base_demo import build_celery_config
        from dagster_quickstart.MQL.hawk import HawkStrategy
    except Exception as exc:
        raise ValueQueryParameterError("Direct Hawk fetch dependencies are unavailable") from exc

    symbols = list(tickers.values())
    if not symbols:
        return empty_direct_source_raw_df()

    strategy = HawkStrategy(config=build_celery_config())
    result = strategy.get_history(
        symbols=symbols,
        fromDate=start_dt or datetime(1970, 1, 1),
        toDate=end_dt or datetime.utcnow(),
        frequency="D",
    )
    raw_df = getattr(result, "payload", None)
    if raw_df is None or raw_df.empty:
        return empty_direct_source_raw_df()

    raw_df = raw_df.copy()
    if isinstance(raw_df.index, pd.DatetimeIndex):
        raw_df.index.names = [ValueColumns.TIMESTAMP]
        raw_df.index = pd.to_datetime(raw_df.index, utc=True)
    else:
        return empty_direct_source_raw_df()

    rename_map = {
        ticker: series_code
        for series_code, ticker in tickers.items()
        if ticker in raw_df.columns
    }
    if not rename_map:
        return empty_direct_source_raw_df()

    raw_df.rename(columns=rename_map, inplace=True)
    return raw_df[list(rename_map.values())]


def fetch_direct_onetick(
    tickers: Dict[str, str],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> pd.DataFrame:
    """
    Fetch raw history data from OneTick.
    """
    try:
        from onetick import OneTick  # type: ignore
    except Exception:
        try:
            from pyeqdr import OneTick  # type: ignore
        except Exception as exc:
            raise ValueQueryParameterError(
                "Direct OneTick fetch requires a OneTick client library"
            ) from exc

    symbols = list(tickers.values())
    if not symbols:
        return empty_direct_source_raw_df()

    client = OneTick()
    raw_df = client.get_history(
        symbols=symbols,
        from_date=start_dt,
        to_date=end_dt,
    )
    if raw_df is None or raw_df.empty:
        return empty_direct_source_raw_df()

    raw_df = raw_df.copy()
    if ValueColumns.TIMESTAMP in raw_df.columns:
        raw_df[ValueColumns.TIMESTAMP] = pd.to_datetime(
            raw_df[ValueColumns.TIMESTAMP],
            utc=True,
        )
        raw_df = raw_df.set_index(ValueColumns.TIMESTAMP)
    elif isinstance(raw_df.index, pd.DatetimeIndex):
        raw_df.index = pd.to_datetime(raw_df.index, utc=True)
    else:
        return empty_direct_source_raw_df()

    raw_df.index.names = [ValueColumns.TIMESTAMP]
    rename_map = {
        ticker: series_code
        for series_code, ticker in tickers.items()
        if ticker in raw_df.columns
    }
    if not rename_map:
        return empty_direct_source_raw_df()

    raw_df.rename(columns=rename_map, inplace=True)
    return raw_df[list(rename_map.values())]


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
        return empty_direct_value_df()
    if tickersource == TickerSource.BLOOMBERG:
        _, field_col = ticker_and_field_columns(TickerSource.BLOOMBERG)
        ticker_fields = {
            str(field_name): build_series_to_ticker_map(group, TickerSource.BLOOMBERG)
            for field_name, group in mapping_df.groupby(field_col)
        }
        raw_out = fetch_direct_bloomberg_tss(ticker_fields, start_dt, end_dt)
    elif tickersource == TickerSource.HAWKEYE:
        raw_out = fetch_direct_hawk(
            build_series_to_ticker_map(mapping_df, TickerSource.HAWKEYE),
            start_dt,
            end_dt,
        )
    elif tickersource in (TickerSource.MDS, TickerSource.ONETICK):
        raw_out = fetch_direct_onetick(
            build_series_to_ticker_map(mapping_df, TickerSource.MDS),
            start_dt,
            end_dt,
        )
    else:
        raise ValueQueryParameterError(
            f"Direct source fetch is not implemented for '{tickersource.value}'"
        )
    out = reshape_direct_source_df(raw_out)
    return filter_and_sort_direct_value_df(out, params)
