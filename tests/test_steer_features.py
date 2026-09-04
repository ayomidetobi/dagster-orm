"""Unit tests for steer.features (log/level switch) and steer.silver (calendar conforming)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.availability.report import PairAvailability
from dagster_quickstart.steer.config import DRIVER_NAMES, StrategyConfig
from dagster_quickstart.steer.source.features import (
    DriverValues,
    build_chn_flows,
    build_chn_offshore_spread,
    build_steer_features,
    compute_realized_volatility,
    fetch_raw_driver_frame,
    required_series_codes,
    resolve_flows_cutover,
    should_use_logged_rate,
)
from dagster_quickstart.steer.source.features import conform_to_business_days

_CHN_DRIVERS = DRIVER_NAMES + ("offshore_spread", "flows")


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

    features = build_steer_features(
        raw, drivers=DRIVER_NAMES, logged_rate_threshold=0.01, vol_window_days=20
    )

    assert "realized_volatility" in features.columns
    assert "is_logged" in features.columns
    assert features["is_logged"].dtype == bool


def test_build_steer_features_raises_on_missing_column():
    raw = pd.DataFrame({"rate": [1.0, 1.1]}, index=pd.bdate_range("2024-01-01", periods=2))

    with pytest.raises(KeyError):
        build_steer_features(raw, drivers=DRIVER_NAMES, logged_rate_threshold=0.01)


def test_build_steer_features_keeps_chns_extra_driver_columns():
    """FEATURE_COLUMNS used to be a fixed 5-driver module constant -- CHN's
    offshore_spread/flows columns would've been silently dropped by
    `raw[list(FEATURE_COLUMNS)]`. drivers= makes the column set per-variant."""
    dates = pd.bdate_range("2024-01-01", periods=10)
    raw = pd.DataFrame(
        {
            "rate": 1.0,
            **{driver: 0.1 for driver in _CHN_DRIVERS},
        },
        index=dates,
    )

    features = build_steer_features(raw, drivers=_CHN_DRIVERS, logged_rate_threshold=0.01)

    assert "offshore_spread" in features.columns
    assert "flows" in features.columns


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


def test_conform_to_business_days_without_primary_column_uses_full_frame_range():
    """Default (no primary_column) behavior, unchanged: the calendar spans
    every column's dates, even a driver with a much longer history than
    the rate -- so the rate ends up null for the driver-only portion."""
    dates = pd.to_datetime(["2020-01-01", "2024-01-01", "2024-01-02"])
    raw = pd.DataFrame(
        {"rate": [None, 1.0, 1.1], "global_equity": [100.0, 105.0, 106.0]}, index=dates
    )

    conformed = conform_to_business_days(raw)

    assert conformed.index.min() <= pd.Timestamp("2020-01-01")
    assert pd.isna(conformed.loc["2020-01-01", "rate"])


def test_conform_to_business_days_primary_column_bounds_the_calendar():
    """A driver ingested back to 1970 (e.g. a long-lived global benchmark)
    shouldn't stretch the calendar decades before the rate itself starts --
    primary_column="rate" bounds the calendar to the rate's own real range,
    so no leading rate=NaN rows appear at all."""
    dates = pd.to_datetime(["2020-01-01", "2024-01-01", "2024-01-02"])
    raw = pd.DataFrame(
        {"rate": [None, 1.0, 1.1], "global_equity": [100.0, 105.0, 106.0]}, index=dates
    )

    conformed = conform_to_business_days(raw, primary_column="rate")

    assert conformed.index.min() == pd.Timestamp("2024-01-01")
    assert not conformed["rate"].isna().any()


class _FakeMetadataFrame:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame


class _FakeFetchDataAPI:
    """Minimal get_values/get_metadata stand-in for fetch_raw_driver_frame."""

    def __init__(self, values: pd.DataFrame, metadata: pd.DataFrame | None = None):
        self._values = values
        self._metadata = (
            metadata if metadata is not None else pd.DataFrame(columns=["series_code", "valid_to"])
        )

    def get_values(self, series_codes):
        columns = [code for code in series_codes if code in self._values.columns]
        return self._values[columns]

    def get_metadata(self, **filters):
        frame = self._metadata
        if "series_code" in filters:
            frame = frame[frame["series_code"].isin(filters["series_code"])]
        return _FakeMetadataFrame(frame.reset_index(drop=True))


def _strategy_config(variant: str, drivers=DRIVER_NAMES) -> StrategyConfig:
    return StrategyConfig(
        variant=variant,
        window_months=12,
        stop_reward_ratio=2.0,
        logged_rate_threshold=0.01,
        drivers=drivers,
        expected_signs={driver: 0 for driver in drivers},
    )


def test_g10_interest_rate_differential_and_yield_curve_or_cds_are_different_series():
    """Both drivers used to be the exact same object -- perfect collinearity.
    They must now be genuinely different series with real, distinct information."""
    dates = pd.bdate_range("2024-01-01", periods=60)
    rng = np.random.default_rng(11)
    values = pd.DataFrame(
        {
            "EURUSD_PX_LAST": 1.1 + np.cumsum(rng.normal(0, 0.001, 60)),
            "MXWO_PX_LAST": 100 + np.cumsum(rng.normal(0, 0.5, 60)),
            "BRENT_PX_LAST": 80 + np.cumsum(rng.normal(0, 0.3, 60)),
            "B_SWAP": 2.0 + np.cumsum(rng.normal(0, 0.01, 60)),
            "Q_SWAP": 1.5 + np.cumsum(rng.normal(0, 0.01, 60)),
            "B_3M": 2.1 + np.cumsum(rng.normal(0, 0.01, 60)),
            "Q_3M": 1.4 + np.cumsum(rng.normal(0, 0.01, 60)),
            "B_10Y": 3.0 + np.cumsum(rng.normal(0, 0.01, 60)),
            "Q_10Y": 2.6 + np.cumsum(rng.normal(0, 0.01, 60)),
            "B_EQ": 800 + np.cumsum(rng.normal(0, 2, 60)),
            "Q_EQ": 2500 + np.cumsum(rng.normal(0, 5, 60)),
        },
        index=dates,
    )
    availability = PairAvailability(
        series_code="EURUSD_PX_LAST",
        variant="G10",
        base_currency="EUR",
        quote_currency="USD",
        resolved={
            ("base", "swap_2y"): "B_SWAP",
            ("quote", "swap_2y"): "Q_SWAP",
            ("base", "rate_3m"): "B_3M",
            ("quote", "rate_3m"): "Q_3M",
            ("base", "yield_10y"): "B_10Y",
            ("quote", "yield_10y"): "Q_10Y",
            ("base", "local_equity"): "B_EQ",
            ("quote", "local_equity"): "Q_EQ",
        },
    )

    features = fetch_raw_driver_frame(
        DriverValues(values), "EURUSD_PX_LAST", _strategy_config("G10"), availability
    )

    ird = features["interest_rate_differential"]
    curve = features["yield_curve_or_cds"]
    assert not ird.equals(curve)
    correlation = ird.astype(float).corr(curve.astype(float))
    assert correlation < 0.9

    expected_ird = values["B_SWAP"] - values["Q_SWAP"]
    expected_curve = (values["B_3M"] - values["B_10Y"]) - (values["Q_3M"] - values["Q_10Y"])
    pd.testing.assert_series_equal(ird, expected_ird, check_names=False)
    pd.testing.assert_series_equal(curve, expected_curve, check_names=False)


def test_g10_local_equity_and_global_drivers_are_log_transformed():
    dates = pd.bdate_range("2024-01-01", periods=10)
    values = pd.DataFrame(
        {
            "EURUSD_PX_LAST": 1.1,
            "MXWO_PX_LAST": 100.0,
            "BRENT_PX_LAST": 80.0,
            "B_SWAP": 2.0,
            "Q_SWAP": 1.5,
            "B_3M": 2.1,
            "Q_3M": 1.4,
            "B_10Y": 3.0,
            "Q_10Y": 2.6,
            "B_EQ": 800.0,
            "Q_EQ": 2500.0,
        },
        index=dates,
    )
    availability = PairAvailability(
        series_code="EURUSD_PX_LAST",
        variant="G10",
        base_currency="EUR",
        quote_currency="USD",
        resolved={
            ("base", "swap_2y"): "B_SWAP",
            ("quote", "swap_2y"): "Q_SWAP",
            ("base", "rate_3m"): "B_3M",
            ("quote", "rate_3m"): "Q_3M",
            ("base", "yield_10y"): "B_10Y",
            ("quote", "yield_10y"): "Q_10Y",
            ("base", "local_equity"): "B_EQ",
            ("quote", "local_equity"): "Q_EQ",
        },
    )

    features = fetch_raw_driver_frame(
        DriverValues(values), "EURUSD_PX_LAST", _strategy_config("G10"), availability
    )

    assert features["global_equity"].iloc[0] == pytest.approx(np.log(100.0))
    assert features["commodity"].iloc[0] == pytest.approx(np.log(80.0))
    assert features["local_equity"].iloc[0] == pytest.approx(np.log(800.0) - np.log(2500.0))


def test_em_yield_curve_or_cds_is_the_non_usd_legs_cds_level_not_a_difference():
    dates = pd.bdate_range("2024-01-01", periods=10)
    values = pd.DataFrame(
        {
            "USDZAR_PX_LAST": 18.5,
            "MXWO_PX_LAST": 100.0,
            "BRENT_PX_LAST": 80.0,
            "USD_SWAP": 1.5,
            "ZAR_SWAP": 7.0,
            "ZAR_CDS": 220.0,
            "USD_EQ": 4000.0,
            "ZAR_EQ": 70000.0,
        },
        index=dates,
    )
    availability = PairAvailability(
        series_code="USDZAR_PX_LAST",
        variant="EM",
        base_currency="USD",
        quote_currency="ZAR",
        resolved={
            ("base", "swap_2y"): "USD_SWAP",
            ("quote", "swap_2y"): "ZAR_SWAP",
            ("base", "local_equity"): "USD_EQ",
            ("quote", "local_equity"): "ZAR_EQ",
            ("quote", "cds_5y"): "ZAR_CDS",
        },
    )

    features = fetch_raw_driver_frame(
        DriverValues(values), "USDZAR_PX_LAST", _strategy_config("EM"), availability
    )

    assert (features["yield_curve_or_cds"] == 220.0).all()
    assert features["local_equity"].iloc[0] == pytest.approx(np.log(70000.0))


def test_missing_role_fills_driver_with_na_never_a_proxy():
    dates = pd.bdate_range("2024-01-01", periods=5)
    values = pd.DataFrame(
        {"EURUSD_PX_LAST": 1.1, "MXWO_PX_LAST": 100.0, "BRENT_PX_LAST": 80.0}, index=dates
    )
    availability = PairAvailability(
        series_code="EURUSD_PX_LAST",
        variant="G10",
        base_currency="EUR",
        quote_currency="USD",
        resolved={},
        missing_reasons={"base:swap_2y": "missing"},
    )

    features = fetch_raw_driver_frame(
        DriverValues(values), "EURUSD_PX_LAST", _strategy_config("G10"), availability
    )

    assert features["local_equity"].isna().all()
    assert features["interest_rate_differential"].isna().all()
    assert features["yield_curve_or_cds"].isna().all()
    # global_equity is never a stand-in for the missing local_equity.
    assert not features["local_equity"].equals(features["global_equity"])


def test_resolve_flows_cutover_reads_the_shared_valid_to():
    metadata = pd.DataFrame(
        {
            "series_code": [
                "SHANGHAI_BUY_FLOWS_PX_LAST",
                "SHENZHEN_BUY_FLOWS_PX_LAST",
                "SHANGHAI_SELL_FLOWS_PX_LAST",
                "SHENZHEN_SELL_FLOWS_PX_LAST",
            ],
            "valid_to": ["2024-08-16"] * 4,
        }
    )

    cutover = resolve_flows_cutover(_FakeFetchDataAPI(pd.DataFrame(), metadata))

    assert cutover == pd.Timestamp("2024-08-16")


def test_build_chn_flows_switches_formula_at_the_cutover():
    dates = pd.bdate_range("2024-08-12", periods=6)  # straddles 2024-08-16
    cutover = pd.Timestamp("2024-08-16")
    ones = pd.Series(1.0, index=dates)

    flows = build_chn_flows(
        shanghai_buy=ones * 10,
        shenzhen_buy=ones * 5,
        shanghai_sell=ones * 3,
        shenzhen_sell=ones * 2,
        shanghai_turnover=ones * 100,
        shenzhen_turnover=ones * 50,
        cutover=cutover,
    )

    pre = flows.loc[flows.index < cutover]
    post = flows.loc[flows.index >= cutover]
    assert (pre == (10 + 5) - (3 + 2)).all()  # net flows
    assert (post == 100 + 50).all()  # total turnover
    assert not (pre == post.iloc[0]).all()


def test_build_chn_offshore_spread_is_offshore_minus_onshore():
    dates = pd.bdate_range("2024-01-01", periods=5)
    offshore = pd.Series([1.0, 1.2, 0.9, 1.1, 1.05], index=dates)
    onshore = pd.Series([0.8, 0.8, 0.8, 0.8, 0.8], index=dates)

    spread = build_chn_offshore_spread(offshore, onshore)

    pd.testing.assert_series_equal(spread, offshore - onshore)
