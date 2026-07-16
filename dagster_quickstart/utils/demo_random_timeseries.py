"""Demo random time series for Hawk (MQL) ingestion stubs."""

from __future__ import annotations

import random
from datetime import datetime
from typing import List, Optional

import pandas as pd

from dagster_quickstart.orm.schema import ValueColumns
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

    Column names are vendor codes (e.g. Hawk fame codes). Matches the calendar grid used by
    wide ingestion (``utc_calendar_days_inclusive``).
    """
    dates = utc_calendar_days_inclusive(start, end)
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates], name=ValueColumns.TIMESTAMP)
    if not column_keys:
        return pd.DataFrame(index=idx)

    rng = random.Random(seed) if seed is not None else random.Random()
    data = {key: [round(rng.uniform(low, high), decimals) for _ in dates] for key in column_keys}
    return pd.DataFrame(data, index=idx)
