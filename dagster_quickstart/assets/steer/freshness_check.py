"""Freshness assessment for steer_silver_prices: is a pair's bronze data fresh for the run date?

"Fresh" means the pair's rate series has a value within FRESHNESS_TOLERANCE_DAYS
of the run date -- generous enough to tolerate a weekend/holiday without
false-failing every Monday, but tight enough to catch upstream ingestion
actually having stopped. A universe partition covers many pairs at once, so
this is a plain (bool, reason) helper -- the asset aggregates results across
every pair in the universe into one AssetCheckResult itself, rather than
this module building one AssetCheckResult per pair.
"""

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd

FRESHNESS_CHECK_NAME = "validate_bronze_freshness"
FRESHNESS_TOLERANCE_DAYS = 3


def assess_freshness(
    raw: Optional[pd.DataFrame],
    *,
    as_of: pd.Timestamp,
    tolerance_days: int = FRESHNESS_TOLERANCE_DAYS,
) -> Tuple[bool, str]:
    """(is_fresh, reason) for one pair's raw frame.

    raw=None or empty means nothing was fetched at all -- always stale
    (there's no bronze data for this pair yet).
    """
    if raw is None or raw.empty:
        return False, "no bronze data"

    latest_timestamp = raw.index.max()
    age_days = (as_of - latest_timestamp).days
    if age_days <= tolerance_days:
        return True, f"{age_days}d old"
    return False, f"{age_days}d old (tolerance {tolerance_days}d)"
