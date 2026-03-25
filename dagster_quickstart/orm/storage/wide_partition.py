"""Wide-format monthly partition merge and timestamp index helpers."""

from datetime import datetime
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd

from dagster_quickstart.orm.schema import ValueColumns
from dagster_quickstart.utils.datetime_utils import normalize_date_to_utc


def normalize_wide_timestamp_index(df: pd.DataFrame) -> pd.DataFrame:
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


def slice_wide_for_calendar_month(wide: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Rows whose UTC calendar month matches year/month."""
    if wide.empty:
        return wide
    mask = (wide.index.year == year) & (wide.index.month == month)
    return wide.loc[mask]


def merge_wide_monthly_partition(
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
        ex = normalize_wide_timestamp_index(ex)
    if not inc.empty:
        inc = normalize_wide_timestamp_index(inc)

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


def wide_frame_covers_utc_dates(
    wide: pd.DataFrame, series_code: str, dates: Iterable[datetime]
) -> bool:
    """True if every date in ``dates`` has a non-null value in ``series_code``."""
    normalized = [normalize_date_to_utc(d) for d in dates]
    if not normalized:
        return True
    if wide.empty or series_code not in wide.columns:
        return False
    idx = pd.DatetimeIndex(normalized, tz="UTC")
    col = wide[series_code].reindex(idx)
    return bool(col.notna().all())
