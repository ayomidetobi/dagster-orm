"""Shape value frames between long-form (storage) and wide-form (display).

Long form: one row per (series_code, timestamp, value) -- what DuckLake
stores and write_values() persists. Wide form: one row per timestamp (as
the index), one column per series_code -- what get_values()/get_last_values()
return for display. pivot_values()/melt_values() convert between the two;
shared by the api/ layer (for reads) and vendors/ (for reshaping raw vendor
responses), so it lives at the package root rather than under api/.
"""

from __future__ import annotations

import pandas as pd

from rewrite.data_api.columns import ValueColumns

VALUE_COLUMNS = [ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]


def pivot_values(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot a long-form (series_code, timestamp, value) frame to wide form.

    Wide form: one row per timestamp (as the index), one column per
    series_code. Matches the old orm/ system's QuerySet.value() shape.

    Storage is append-only (DuckLake snapshots every write instead of
    upserting), so the same series/timestamp can legitimately appear more
    than once if a series was re-fetched and re-saved. Uses pivot_table with
    aggfunc="last" rather than pivot() so that's tolerated (keeping the last
    matching row) instead of raising "Index contains duplicate entries".
    """

    if df.empty:
        return df

    return df.pivot_table(
        index=ValueColumns.TIMESTAMP,
        columns=ValueColumns.SERIES_CODE,
        values=ValueColumns.VALUE,
        aggfunc="last",
    ).sort_index()


def melt_values(df: pd.DataFrame) -> pd.DataFrame:
    """Melt a wide (DatetimeIndex x series_code columns) frame to long form.

    Inverse of pivot_values() -- e.g. turns a get_values()-shaped result (or
    a raw vendor response) back into (series_code, timestamp, value) rows.
    Values are daily, so the timestamp is normalized to a plain date
    (tz-stripped, time-of-day dropped).
    """

    if df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return pd.DataFrame(columns=VALUE_COLUMNS)

    normalized = df.copy()
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
    timestamps = pd.to_datetime(out[ValueColumns.TIMESTAMP])
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_convert(None)
    out[ValueColumns.TIMESTAMP] = timestamps.dt.normalize()
    return out[VALUE_COLUMNS]
