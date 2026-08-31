"""Unit tests for steer.features (log/level switch) and steer.silver (calendar conforming)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.steer.features import (
    build_steer_features,
    compute_realized_volatility,
    should_use_logged_rate,
)
from dagster_quickstart.steer.silver import conform_to_business_days


def test_should_use_logged_rate_false_before_full_window():
    rate = pd.Series([1.0] * 5, index=pd.bdate_range("2024-01-01", periods=5))
    volatility = compute_realized_volatility(rate, window_days=20)

    assert should_use_logged_rate(volatility, as_of=rate.index[-1], threshold=0.01) is False


def test_should_use_logged_rate_true_when_volatility_exceeds_threshold():
    dates = pd.bdate_range("2024-01-01", periods=40)
    rng = np.random.default_rng(3)
    # Deliberately volatile: ~3% daily moves, well above a 1% threshold.
    rate = pd.Series(1.0 + np.cumsum(rng.normal(0, 0.03, 40)), index=dates)
    volatility = compute_realized_volatility(rate, window_days=20)

    assert should_use_logged_rate(volatility, as_of=dates[-1], threshold=0.01) is True


def test_should_use_logged_rate_false_when_volatility_below_threshold():
    dates = pd.bdate_range("2024-01-01", periods=40)
    # Perfectly flat -- zero volatility, well below any positive threshold.
    rate = pd.Series([1.0] * 40, index=dates)
    volatility = compute_realized_volatility(rate, window_days=20)

    assert should_use_logged_rate(volatility, as_of=dates[-1], threshold=0.0001) is False


def test_build_steer_features_adds_is_logged_and_volatility_columns():
    dates = pd.bdate_range("2024-01-01", periods=40)
    rng = np.random.default_rng(5)
    raw = pd.DataFrame(
        {
            "rate": 1.0 + np.cumsum(rng.normal(0, 0.001, 40)),
            "interest_rate_differential": rng.normal(size=40),
            "yield_curve_or_cds": rng.normal(size=40),
            "local_equity": rng.normal(size=40),
            "global_equity": rng.normal(size=40),
            "commodity": rng.normal(size=40),
        },
        index=dates,
    )

    features = build_steer_features(raw, logged_rate_threshold=0.01, vol_window_days=20)

    assert "realized_volatility" in features.columns
    assert "is_logged" in features.columns
    assert features["is_logged"].dtype == bool


def test_build_steer_features_raises_on_missing_column():
    raw = pd.DataFrame({"rate": [1.0, 1.1]}, index=pd.bdate_range("2024-01-01", periods=2))

    with pytest.raises(KeyError):
        build_steer_features(raw, logged_rate_threshold=0.01)


def test_conform_to_business_days_forward_fills_short_gaps():
    dates = pd.to_datetime(
        ["2024-01-01", "2024-01-02", "2024-01-04"]
    )  # Jan 3 missing (a Wed -> real gap)
    raw = pd.DataFrame({"rate": [1.0, 1.1, 1.3]}, index=dates)

    conformed = conform_to_business_days(raw, max_forward_fill_days=3)

    assert pd.Timestamp("2024-01-03") in conformed.index
    assert conformed.loc["2024-01-03", "rate"] == pytest.approx(1.1)  # forward-filled from Jan 2


def test_conform_to_business_days_leaves_long_gaps_null():
    dates = pd.to_datetime(["2024-01-01", "2024-01-15"])  # ~2 week gap
    raw = pd.DataFrame({"rate": [1.0, 1.5]}, index=dates)

    conformed = conform_to_business_days(raw, max_forward_fill_days=3)

    mid_gap = pd.Timestamp("2024-01-10")
    assert mid_gap in conformed.index
    assert pd.isna(conformed.loc[mid_gap, "rate"])
