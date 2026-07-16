"""Placeholder random demo data for vendor clients.

Until real vendor integrations (Bloomberg/pyeqdr, Hawk, MDS/OneTick) are
wired in, every vendor client returns random values within a range for the
requested series. Swap a client's fetch() body for a real SDK call when one
becomes available -- the tickers/start/end contract stays the same.
"""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

DEFAULT_LOOKBACK_DAYS = 90
DEFAULT_LOW = 100.0
DEFAULT_HIGH = 200.0


def demo_random_wide_frame(
    tickers: Sequence[str],
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    low: float = DEFAULT_LOW,
    high: float = DEFAULT_HIGH,
    seed: int | None = None,
) -> pd.DataFrame:
    """Build a wide daily DataFrame of random values in [low, high) for each ticker."""

    if not tickers:
        return pd.DataFrame()

    resolved_end = end or datetime.now()
    resolved_start = start or (resolved_end - timedelta(days=DEFAULT_LOOKBACK_DAYS))

    dates = pd.date_range(resolved_start, resolved_end, freq="D", tz="UTC").normalize()
    rng = random.Random(seed)

    data = {ticker: [round(rng.uniform(low, high), 6) for _ in dates] for ticker in tickers}
    return pd.DataFrame(data, index=dates)


def fetch_demo_values(
    vendor: str,
    tickers: Mapping[str, str],
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    """Return a wide (DatetimeIndex, series_code columns) demo frame for a vendor client."""

    if not tickers:
        return pd.DataFrame()

    logger.info("vendor_fetch_started", vendor=vendor, series_count=len(tickers), demo_data=True)

    wide = demo_random_wide_frame(list(tickers.values()), start, end)

    rename_map = {
        ticker: series_code for series_code, ticker in tickers.items() if ticker in wide.columns
    }
    if not rename_map:
        return pd.DataFrame()

    return wide.rename(columns=rename_map)[list(rename_map.values())]
