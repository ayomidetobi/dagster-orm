"""Shape value frames for the public API (long-form storage -> wide-form display)."""

from __future__ import annotations

import pandas as pd

from rewrite.data_api.columns import ValueColumns


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
