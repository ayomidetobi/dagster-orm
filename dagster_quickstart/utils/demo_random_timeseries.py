"""Shared demo random time series for PyPDL (Bloomberg) and Hawk (MQL) ingestion stubs."""

from __future__ import annotations

import random
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.schema import DataPoint, ValueColumns
from dagster_quickstart.utils.datetime_utils import utc_calendar_days_inclusive


def demo_random_wide_frame(
    start: datetime,
    end: datetime,
    column_keys: List[str],
    *,
    low: float = 100.0,
    high: float = 200.0,
    decimals: int = 6,
    seed: Optional[int] = None,
) -> pd.DataFrame:
    """Build a wide daily DataFrame of random values (UTC calendar days, inclusive ``end``).

    Column names are vendor codes (Bloomberg tickers, Hawk fame codes, etc.). Matches the
    calendar grid used by wide ingestion (``utc_calendar_days_inclusive``).

    Args:
        start: Range start (date portion used; normalized in ``utc_calendar_days_inclusive``).
        end: Range end, inclusive of that calendar day.
        column_keys: One series per column (e.g. tickers or fame codes).
        low, high: Uniform draw bounds (inclusive).
        decimals: Rounding for floats (same style as the former PyPDL dummy branch).
        seed: Optional RNG seed for reproducible demos/tests.

    Returns:
        DataFrame indexed by timestamp (name ``ValueColumns.TIMESTAMP``), one float column per key.
    """
    dates = utc_calendar_days_inclusive(start, end)
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates], name=ValueColumns.TIMESTAMP)
    if not column_keys:
        return pd.DataFrame(index=idx)

    rng = random.Random(seed) if seed is not None else random.Random()
    data = {
        key: [round(rng.uniform(low, high), decimals) for _ in dates] for key in column_keys
    }
    return pd.DataFrame(data, index=idx)


def demo_wide_frame_to_pypdl_by_code(wide: pd.DataFrame) -> Dict[str, List[DataPoint]]:
    """Convert a wide demo frame to PyPDL-style ``data_code -> [DataPoint, ...]``."""
    out: Dict[str, List[DataPoint]] = {}
    if wide.empty or not len(wide.columns):
        return out
    for col in wide.columns:
        key = str(col)
        points: List[DataPoint] = []
        for ts, val in wide[key].items():
            t = pd.Timestamp(ts).to_pydatetime()
            points.append({"timestamp": t, "value": float(val)})
        out[key] = points
    return out
