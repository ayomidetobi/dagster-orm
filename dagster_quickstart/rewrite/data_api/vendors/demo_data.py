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
    """Return a wide (DatetimeIndex, series_code columns) demo frame for a vendor client.

    `tickers` is series_code -> vendor ticker; several series_codes can
    legitimately share the same ticker (this catalog has several
    series_codes aliasing the same real-world instrument, e.g. two AUDJPY
    series with different suffixes -- see rewrite/data_api/dataset/fx.py's
    docstrings). Each ticker's simulated price series is generated once
    and broadcast to every series_code that references it -- generating
    one demo_random_wide_frame column per *ticker* and keying the return
    frame by series_code (not by ticker) is what makes that safe: a naive
    ticker-keyed dict would silently collapse duplicate series_codes down
    to whichever one was iterated last.
    """

    if not tickers:
        return pd.DataFrame()

    logger.info("vendor_fetch_started", vendor=vendor, series_count=len(tickers), demo_data=True)

    unique_tickers = sorted(set(tickers.values()))
    wide = demo_random_wide_frame(unique_tickers, start, end)

    columns = {
        series_code: wide[ticker]
        for series_code, ticker in tickers.items()
        if ticker in wide.columns
    }
    if not columns:
        return pd.DataFrame()

    return pd.DataFrame(columns, index=wide.index)
