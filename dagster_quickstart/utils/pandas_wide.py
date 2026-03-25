"""Pandas helpers for wide time-series frames (API boundary shaping only)."""

from typing import Any, Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.schema import ValueColumns
from dagster_quickstart.utils.datetime_utils import ensure_utc, normalize_date_to_utc


def select_series_columns_as_long_df(
    wide_df: pd.DataFrame,
    series_codes: List[str],
    start: Optional[Any] = None,
    end: Optional[Any] = None,
) -> pd.DataFrame:
    """From a wide table (timestamp column + series columns), return long rows for API use."""
    empty = pd.DataFrame(
        columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
    )
    if wide_df.empty or not series_codes or ValueColumns.TIMESTAMP not in wide_df.columns:
        return empty

    work = wide_df.copy()
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


def series_points_dict_to_wide_dataframe(
    series_code_to_points: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    """Build wide frame: UTC DatetimeIndex, one column per series from point lists."""
    if not series_code_to_points:
        return pd.DataFrame()

    all_ts = set()
    for points in series_code_to_points.values():
        for p in points:
            all_ts.add(pd.Timestamp(ensure_utc(p["timestamp"])).normalize())

    if not all_ts:
        return pd.DataFrame()

    sorted_ts = sorted(all_ts)
    idx = pd.DatetimeIndex(sorted_ts, tz="UTC")
    data: Dict[str, List[Any]] = {}
    for sc, points in series_code_to_points.items():
        by_t = {
            pd.Timestamp(ensure_utc(p["timestamp"])).normalize(): p.get("value") for p in points
        }
        data[sc] = [by_t.get(t, float("nan")) for t in sorted_ts]

    out = pd.DataFrame(data, index=idx)
    out.index.name = ValueColumns.TIMESTAMP
    return out.sort_index()
