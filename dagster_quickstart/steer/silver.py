"""Silver-layer conforming: align raw ingested series onto one clean business-day calendar.

Bronze (values, via rewrite/data_api) stores whatever timestamps each
vendor happened to report -- different series can have gaps on different
holidays, occasional missing days, or (rarely) duplicate/near-duplicate
timestamps. This conforms a pair's rate + driver series onto one shared
business-day index before anything touches the regression, so
estimate_steer never has to reason about ragged/misaligned inputs itself.
"""

from __future__ import annotations

import pandas as pd

MAX_FORWARD_FILL_DAYS = 3


def conform_to_business_days(
    raw: pd.DataFrame,
    *,
    max_forward_fill_days: int = MAX_FORWARD_FILL_DAYS,
) -> pd.DataFrame:
    """Reindex `raw` (DatetimeIndex, one column per series) onto a business-day calendar.

    A short gap (a real holiday, a vendor's one-off missing day) is
    forward-filled up to `max_forward_fill_days` -- the last known value
    carries forward, which is standard practice for a market that simply
    didn't trade that day. A gap longer than that is left as NaN rather
    than silently carrying a stale price forward indefinitely; downstream,
    estimate_steer's window .dropna() naturally excludes those rows.
    """
    if raw.empty:
        return raw

    business_days = pd.bdate_range(raw.index.min(), raw.index.max())
    conformed = raw.reindex(business_days)
    conformed = conformed.ffill(limit=max_forward_fill_days)
    conformed.index.name = raw.index.name
    return conformed
