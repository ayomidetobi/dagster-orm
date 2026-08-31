"""Gold-layer STEER feature construction: raw driver series -> a model-ready table.

Turns a wide frame of already-conformed raw series (see
assets/steer/silver_asset.py for the silver-layer alignment step) into the
per-pair STEER feature table: the 5 canonical drivers plus a rolling
`is_logged` flag, one row per date.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from dagster_quickstart.steer.config import DRIVER_NAMES, StrategyConfig
from dagster_quickstart.steer.discovery import PairAvailability

RATE_COLUMN = "rate"
REALIZED_VOLATILITY_COLUMN = "realized_volatility"
IS_LOGGED_COLUMN = "is_logged"

FEATURE_COLUMNS = (RATE_COLUMN,) + DRIVER_NAMES


def compute_realized_volatility(rate: pd.Series, *, window_days: int) -> pd.Series:
    """Trailing rolling mean absolute daily % change of `rate`, in decimal terms (0.01 == 1%).

    NaN for the first `window_days` rows of any series (not enough history
    yet for a full window) -- callers should treat NaN as "not logged" (see
    should_use_logged_rate), not as zero volatility.
    """
    return rate.pct_change().abs().rolling(window_days, min_periods=window_days).mean()


def should_use_logged_rate(
    realized_volatility: pd.Series,
    *,
    as_of: pd.Timestamp,
    threshold: float,
) -> bool:
    """True if `realized_volatility` as of `as_of` exceeds `threshold` (log-space regression); False otherwise.

    False (level regression) whenever there isn't a full volatility window
    yet -- a conservative default rather than guessing log vs. level from
    partial data.
    """
    as_of = pd.Timestamp(as_of)
    trailing = realized_volatility.loc[:as_of]
    if trailing.empty or pd.isna(trailing.iloc[-1]):
        return False
    return bool(trailing.iloc[-1] > threshold)


def build_steer_features(
    raw: pd.DataFrame,
    *,
    logged_rate_threshold: float,
    vol_window_days: int = 20,
) -> pd.DataFrame:
    """Build the gold-layer STEER feature table for one currency pair.

    `raw` must be indexed by date and have one column per FEATURE_COLUMNS
    (rate + the 5 DRIVER_NAMES) -- already resolved from series_codes and
    aligned/conformed (see assets/steer/silver_asset.py). Adds
    realized_volatility and is_logged (recomputed per day, so a pair can
    cross the log/level threshold over time -- estimate_steer/
    cointegration_test are told which regime applies for the specific
    `as_of` they're evaluating, via that day's is_logged value).

    Raises KeyError if `raw` is missing any of FEATURE_COLUMNS -- validated
    more thoroughly downstream by STEER_FEATURES_SCHEMA (see
    steer/schemas.py) as a Dagster asset check.
    """
    missing = [column for column in FEATURE_COLUMNS if column not in raw.columns]
    if missing:
        raise KeyError(f"raw is missing required column(s): {missing}")

    features = raw[list(FEATURE_COLUMNS)].copy()
    volatility = compute_realized_volatility(features[RATE_COLUMN], window_days=vol_window_days)
    features[REALIZED_VOLATILITY_COLUMN] = volatility
    features[IS_LOGGED_COLUMN] = volatility > logged_rate_threshold
    features[IS_LOGGED_COLUMN] = features[IS_LOGGED_COLUMN].fillna(False)
    return features


def fetch_raw_driver_frame(
    data_api: Any,
    series_code: str,
    strategy_config: StrategyConfig,
    availability: PairAvailability,
    *,
    start: Any = None,
    end: Any = None,
) -> pd.DataFrame:
    """Fetch one pair's rate + drivers from DuckLake and rename to FEATURE_COLUMNS.

    `data_api` is a rewrite.data_api.api.data_api.DataAPI (typed loosely
    here to avoid a Dagster-free module importing the DataAPI stack).
    Callers should already have checked `availability.blocked` (see
    assets/steer/silver_asset.py) -- this fetches whatever it can
    regardless, filling anything unavailable (local_equity when
    availability.local_equity_available is False;
    interest_rate_differential/yield_curve_or_cds when
    availability.rate_data_available is False) with NaN, so a caller that
    calls this on a blocked pair anyway still gets a well-formed (if
    unusable) frame rather than a crash.

    global_equity/commodity are single series named in strategy_config
    (curated, not per-pair -- see StrategyConfig's docstring). rate is
    `series_code` itself. interest_rate_differential is
    base_rate_series - quote_rate_series (both from `availability`, when
    present); yield_curve_or_cds currently reuses the same two series
    (this catalog only has flat sovereign-yield points, not a real
    curve-slope or CDS series -- a documented simplification, not a
    fabrication of missing data). local_equity is
    base_equity_series - quote_equity_series (both from `availability`,
    when present) -- a raw index-level difference, the same simplification
    already used for interest_rate_differential and already applied to
    global_equity/commodity (neither of which is normalized before OLS
    either).
    """
    series_by_column = {
        RATE_COLUMN: series_code,
        "global_equity": strategy_config.global_equity_series,
        "commodity": strategy_config.commodity_series,
    }
    if availability.rate_data_available:
        assert availability.base_rate_series is not None
        assert availability.quote_rate_series is not None
        series_by_column["_base_rate"] = availability.base_rate_series
        series_by_column["_quote_rate"] = availability.quote_rate_series
    if availability.local_equity_available:
        assert availability.base_equity_series is not None
        assert availability.quote_equity_series is not None
        series_by_column["_base_equity"] = availability.base_equity_series
        series_by_column["_quote_equity"] = availability.quote_equity_series

    column_by_series: dict[str, str] = {}
    for column, code in series_by_column.items():
        column_by_series.setdefault(code, column)

    wide = data_api.get_values(list(column_by_series))
    if start is not None:
        wide = wide.loc[wide.index >= pd.Timestamp(start)]
    if end is not None:
        wide = wide.loc[wide.index <= pd.Timestamp(end)]
    if wide.empty:
        return wide

    renamed = wide.rename(columns=column_by_series)

    features = pd.DataFrame(index=renamed.index)
    features[RATE_COLUMN] = renamed.get(RATE_COLUMN)
    features["global_equity"] = renamed.get("global_equity")
    features["commodity"] = renamed.get("commodity")
    if (
        availability.local_equity_available
        and "_base_equity" in renamed
        and "_quote_equity" in renamed
    ):
        features["local_equity"] = renamed["_base_equity"] - renamed["_quote_equity"]
    else:
        features["local_equity"] = pd.NA
    if availability.rate_data_available and "_base_rate" in renamed and "_quote_rate" in renamed:
        differential = renamed["_base_rate"] - renamed["_quote_rate"]
        features["interest_rate_differential"] = differential
        features["yield_curve_or_cds"] = differential
    else:
        features["interest_rate_differential"] = pd.NA
        features["yield_curve_or_cds"] = pd.NA

    return features[list(FEATURE_COLUMNS)]
