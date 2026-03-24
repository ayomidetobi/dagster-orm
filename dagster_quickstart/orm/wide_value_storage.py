"""Wide-format value storage helpers: pivot, monthly partition merge, date utilities."""

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from dagster_quickstart.orm.schema import ValueColumns
from dagster_quickstart.utils.datetime_utils import normalize_date_to_utc


def iter_year_months(start_date: datetime, end_date: datetime) -> List[Tuple[int, int]]:
    """Inclusive calendar (year, month) pairs from start through end."""
    start = normalize_date_to_utc(start_date)
    end = normalize_date_to_utc(end_date)
    y, m = start.year, start.month
    out: List[Tuple[int, int]] = []
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def normalize_timestamp_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DatetimeIndex UTC, normalized to midnight, sorted."""
    if df.index.name != ValueColumns.TIMESTAMP and ValueColumns.TIMESTAMP in df.columns:
        df = df.set_index(ValueColumns.TIMESTAMP)
    idx = pd.DatetimeIndex(df.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")
    idx = idx.normalize()
    out = df.copy()
    out.index = idx
    out.index.name = ValueColumns.TIMESTAMP
    return out.sort_index()


def pivot_long_to_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long (timestamp, series_code, value) to wide matrix; duplicate keys keep last."""
    if long_df.empty:
        return pd.DataFrame()
    work = long_df.copy()
    work[ValueColumns.TIMESTAMP] = pd.to_datetime(
        work[ValueColumns.TIMESTAMP], utc=True
    ).dt.normalize()
    work = work.sort_values([ValueColumns.TIMESTAMP, ValueColumns.SERIES_CODE])
    work = work.drop_duplicates(
        [ValueColumns.TIMESTAMP, ValueColumns.SERIES_CODE], keep="last"
    )
    wide = work.pivot(
        index=ValueColumns.TIMESTAMP,
        columns=ValueColumns.SERIES_CODE,
        values=ValueColumns.VALUE,
    )
    wide.index = pd.to_datetime(wide.index, utc=True)
    wide.index = wide.index.normalize()
    wide.index.name = ValueColumns.TIMESTAMP
    wide.columns.name = None
    return wide.sort_index()


def slice_wide_for_month(wide: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Rows whose UTC calendar month matches year/month."""
    if wide.empty:
        return wide
    mask = (wide.index.year == year) & (wide.index.month == month)
    return wide.loc[mask]


def merge_wide_partition(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    strip_date_range: Optional[Tuple[datetime, datetime]],
) -> pd.DataFrame:
    """Merge incoming wide slice into existing monthly partition.

    Args:
        existing: Prior wide data (DatetimeIndex UTC).
        incoming: New rows/columns for the same partition.
        strip_date_range: If set, drop existing rows whose UTC date falls in
            [start, end] inclusive before merging (force-refresh semantics).

    Returns:
        Sorted wide DataFrame with no duplicate index labels.
    """
    ex = existing if existing is not None else pd.DataFrame()
    inc = incoming if incoming is not None else pd.DataFrame()

    if not ex.empty:
        ex = normalize_timestamp_index(ex)
    if not inc.empty:
        inc = normalize_timestamp_index(inc)

    if strip_date_range is not None:
        rs, re = strip_date_range
        rs0 = normalize_date_to_utc(rs)
        re0 = normalize_date_to_utc(re)
        if not ex.empty:
            ts = ex.index.normalize()
            keep = ~((ts >= rs0) & (ts <= re0))
            ex = ex.loc[keep]

    if ex.empty:
        out = inc.sort_index() if not inc.empty else pd.DataFrame()
    elif inc.empty:
        out = ex.sort_index()
    else:
        all_idx = ex.index.union(inc.index).sort_values()
        out = ex.reindex(all_idx)
        for col in inc.columns:
            if col not in out.columns:
                out[col] = np.nan
            out.loc[inc.index, col] = inc[col].values
        out = out.sort_index()

    if out.empty:
        return out
    out = out.groupby(level=0).last()
    out.index.name = ValueColumns.TIMESTAMP
    return out


def dates_by_year_month(
    start_date: datetime, end_date: datetime
) -> Dict[Tuple[int, int], List[datetime]]:
    """Map (year, month) to UTC midnights in [start_date, end_date] for that month."""
    start = normalize_date_to_utc(start_date)
    end = normalize_date_to_utc(end_date)
    buckets: Dict[Tuple[int, int], List[datetime]] = {}
    current = start
    while current <= end:
        key = (current.year, current.month)
        buckets.setdefault(key, []).append(current)
        current = current + timedelta(days=1)
    return buckets


def wide_table_to_long(
    df: pd.DataFrame,
    series_codes: List[str],
    start: Optional[Any] = None,
    end: Optional[Any] = None,
) -> pd.DataFrame:
    """Turn a wide monthly/glob-read frame into long ``series_code, timestamp, value`` rows."""
    empty = pd.DataFrame(
        columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
    )
    if df.empty or not series_codes or ValueColumns.TIMESTAMP not in df.columns:
        return empty

    work = df.copy()
    work[ValueColumns.TIMESTAMP] = pd.to_datetime(
        work[ValueColumns.TIMESTAMP], utc=True, errors="coerce"
    )
    work = work.dropna(subset=[ValueColumns.TIMESTAMP])

    if start is not None:
        t0 = pd.Timestamp(normalize_date_to_utc(start))
        work = work.loc[work[ValueColumns.TIMESTAMP] >= t0]
    if end is not None:
        t1 = pd.Timestamp(normalize_date_to_utc(end))
        work = work.loc[work[ValueColumns.TIMESTAMP] <= t1]

    value_vars = [c for c in series_codes if c in work.columns]
    if not value_vars:
        return empty

    long_df = work.melt(
        id_vars=[ValueColumns.TIMESTAMP],
        value_vars=value_vars,
        var_name=ValueColumns.SERIES_CODE,
        value_name=ValueColumns.VALUE,
    )
    return long_df[
        [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
    ].sort_values(ValueColumns.TIMESTAMP)


def wide_series_covers_dates(
    wide: pd.DataFrame, series_code: str, dates: Iterable[datetime]
) -> bool:
    """True if every date in ``dates`` has a non-null value in ``series_code``."""
    dlist = [normalize_date_to_utc(d) for d in dates]
    if not dlist:
        return True
    if wide.empty or series_code not in wide.columns:
        return False
    idx = pd.DatetimeIndex(dlist, tz="UTC")
    col = wide[series_code].reindex(idx)
    return bool(col.notna().all())
