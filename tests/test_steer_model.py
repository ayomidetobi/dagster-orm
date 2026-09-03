"""Tests for steer.model.Steer/SteerPanel -- the model-object facade.

Acceptance criterion 1 is the important one: Steer.fit() must produce EXACTLY the same numbers
as materializing the real asset graph (steer_data_availability -> ... -> steer_signal), for one
G10, one EM, and one CHN pair, since it's a facade over the same functions, not a second
implementation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from dagster import DagsterInstance, materialize

from dagster_quickstart.assets.steer.availability_asset import steer_data_availability
from dagster_quickstart.assets.steer.cointegration_asset import steer_cointegration
from dagster_quickstart.assets.steer.estimate_asset import steer_estimate
from dagster_quickstart.assets.steer.gold_features_asset import steer_features
from dagster_quickstart.assets.steer.signal_asset import steer_signal
from dagster_quickstart.assets.steer.silver_asset import steer_silver_prices
from dagster_quickstart.steer.config import DRIVER_NAMES, StrategyConfig
from dagster_quickstart.steer.model import Steer
from tests.test_steer_assets import (
    FakeRewriteDataAPIResource,
    FakeSteerConfigResource,
    _unblocked_g10_metadata,
    _unblocked_g10_values,
)


def _materialize_asset_pipeline(metadata: pd.DataFrame, values: pd.DataFrame, strategy_config: StrategyConfig):
    resources = {
        "rewrite_data_api": FakeRewriteDataAPIResource(metadata, values),
        "steer_config": FakeSteerConfigResource(strategy_config),
    }
    result = materialize(
        [
            steer_data_availability,
            steer_silver_prices,
            steer_features,
            steer_cointegration,
            steer_estimate,
            steer_signal,
        ],
        resources=resources,
        partition_key=strategy_config.universe,
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success
    return result, resources["rewrite_data_api"].api


def _g10_strategy_config() -> StrategyConfig:
    return StrategyConfig(
        universe="G10",
        window_months=12,
        stop_reward_ratio=2.0,
        logged_rate_threshold=0.01,
        min_observations=60,
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": 1,
            "global_equity": 1,
            "commodity": 1,
        },
    )


def _em_metadata() -> pd.DataFrame:
    rows = [
        {"series_code": "USDZAR_PX_LAST", "asset_class": "Currency", "sub_asset_class": "FX Spot",
         "market_development": "EM", "currency": "ZAR", "tenor": None, "market_segment": None},
        {"series_code": "MXWO_PX_LAST", "asset_class": "Equity", "sub_asset_class": "Equity Index",
         "market_development": "GLOBAL", "currency": "USD", "tenor": None, "market_segment": "Global"},
        {"series_code": "BRENT_PX_LAST", "asset_class": "Commodity", "sub_asset_class": "Crude Oil",
         "market_development": "GLOBAL", "currency": "USD", "tenor": None, "market_segment": None},
    ]
    for ccy in ("USD", "ZAR"):
        rows.append({"series_code": f"{ccy}_SWAP", "asset_class": "Fixed Income",
                     "sub_asset_class": "Interest Rate Swap", "market_development": "EM",
                     "currency": ccy, "tenor": "2Y", "market_segment": None})
        rows.append({"series_code": f"{ccy}_EQ", "asset_class": "Equity",
                     "sub_asset_class": "Equity Index", "market_development": "EM",
                     "currency": ccy, "tenor": None, "market_segment": "Local"})
    rows.append({"series_code": "ZAR_CDS", "asset_class": "Credit", "sub_asset_class": "Sovereign CDS",
                 "market_development": "EM", "currency": "ZAR", "tenor": "5Y", "market_segment": None})
    return pd.DataFrame(rows)


def _em_values() -> pd.DataFrame:
    rng = np.random.default_rng(31)
    n = 400
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=n)

    usd_swap = 1.5 + np.cumsum(rng.normal(0, 0.01, n))
    zar_swap = 7.0 + np.cumsum(rng.normal(0, 0.02, n))
    usd_eq = 2500 + np.cumsum(rng.normal(0, 5, n))
    zar_eq = 70000 + np.cumsum(rng.normal(0, 200, n))
    zar_cds = 220 + np.cumsum(rng.normal(0, 2, n))
    mxwo = 100 + np.cumsum(rng.normal(0, 0.5, n))
    brent = 80 + np.cumsum(rng.normal(0, 0.3, n))

    ird = usd_swap - zar_swap
    local_eq = np.log(zar_eq)
    rate = 18.0 - 0.5 * ird + 0.02 * zar_cds + 0.5 * local_eq + rng.normal(0, 0.05, n)

    series = {
        "USDZAR_PX_LAST": rate,
        "USD_SWAP": usd_swap,
        "ZAR_SWAP": zar_swap,
        "USD_EQ": usd_eq,
        "ZAR_EQ": zar_eq,
        "ZAR_CDS": zar_cds,
        "MXWO_PX_LAST": mxwo,
        "BRENT_PX_LAST": brent,
    }
    rows = []
    for series_code, vals in series.items():
        for date, value in zip(dates, vals):
            rows.append({"series_code": series_code, "timestamp": date, "value": float(value)})
    return pd.DataFrame(rows)


def _em_strategy_config() -> StrategyConfig:
    return StrategyConfig(
        universe="EM",
        window_months=6,
        stop_reward_ratio=1.0,
        logged_rate_threshold=0.0025,
        min_observations=60,
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": 1,
            "global_equity": 1,
            "commodity": 1,
        },
    )


_CHN_FLOW_BUY_SELL = (
    "SHANGHAI_BUY_FLOWS_PX_LAST", "SHENZHEN_BUY_FLOWS_PX_LAST",
    "SHANGHAI_SELL_FLOWS_PX_LAST", "SHENZHEN_SELL_FLOWS_PX_LAST",
)
_CHN_FLOW_TURNOVER = ("SHANGHAI_FLOWS_TURNOVER_PX_LAST", "SHENZHEN_FLOWS_TURNOVER_PX_LAST")


def _chn_metadata() -> pd.DataFrame:
    rows = [
        {"series_code": "USDCNH_PX_LAST", "asset_class": "Currency", "sub_asset_class": "FX Spot",
         "market_development": "CHN", "currency": "CNH", "tenor": None, "market_segment": None},
        {"series_code": "MXWO_PX_LAST", "asset_class": "Equity", "sub_asset_class": "Equity Index",
         "market_development": "GLOBAL", "currency": "USD", "tenor": None, "market_segment": "Global"},
        {"series_code": "BRENT_PX_LAST", "asset_class": "Commodity", "sub_asset_class": "Crude Oil",
         "market_development": "GLOBAL", "currency": "USD", "tenor": None, "market_segment": None},
    ]
    for ccy in ("USD", "CNH"):
        rows.append({"series_code": f"{ccy}_SWAP", "asset_class": "Fixed Income",
                     "sub_asset_class": "Interest Rate Swap", "market_development": "CHN",
                     "currency": ccy, "tenor": "2Y", "market_segment": None})
        rows.append({"series_code": f"{ccy}_EQ", "asset_class": "Equity",
                     "sub_asset_class": "Equity Index", "market_development": "CHN",
                     "currency": ccy, "tenor": None, "market_segment": "Local"})
    rows.append({"series_code": "CNH_CDS", "asset_class": "Credit", "sub_asset_class": "Sovereign CDS",
                 "market_development": "CHN", "currency": "CNH", "tenor": "5Y", "market_segment": None})
    for code in _CHN_FLOW_BUY_SELL:
        rows.append({"series_code": code, "asset_class": "Equity", "sub_asset_class": "Equity Flow",
                     "market_development": "CHN", "currency": "CNY", "tenor": None,
                     "market_segment": None, "valid_to": "2024-08-16"})
    for code in _CHN_FLOW_TURNOVER:
        rows.append({"series_code": code, "asset_class": "Equity", "sub_asset_class": "Equity Flow",
                     "market_development": "CHN", "currency": "CNY", "tenor": None,
                     "market_segment": None, "valid_to": None})
    rows.append({"series_code": "OFFSHORE_SPREAD_PX_LAST", "asset_class": "Currency",
                 "sub_asset_class": "FX Forward", "market_development": "CHN", "currency": "CNH",
                 "tenor": "3M", "market_segment": "Offshore"})
    rows.append({"series_code": "ONSHORE_SPREAD_PX_LAST", "asset_class": "Currency",
                 "sub_asset_class": "FX Forward", "market_development": "CHN", "currency": "CNY",
                 "tenor": "3M", "market_segment": "Onshore"})
    return pd.DataFrame(rows)


def _chn_values() -> pd.DataFrame:
    rng = np.random.default_rng(41)
    n = 400
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=n)

    usd_swap = 1.5 + np.cumsum(rng.normal(0, 0.01, n))
    cnh_swap = 2.0 + np.cumsum(rng.normal(0, 0.01, n))
    usd_eq = 2500 + np.cumsum(rng.normal(0, 5, n))
    cnh_eq = 3500 + np.cumsum(rng.normal(0, 10, n))
    cnh_cds = 60 + np.cumsum(rng.normal(0, 1, n))
    mxwo = 100 + np.cumsum(rng.normal(0, 0.5, n))
    brent = 80 + np.cumsum(rng.normal(0, 0.3, n))
    offshore = 100 + np.cumsum(rng.normal(0, 1, n))
    onshore = 90 + np.cumsum(rng.normal(0, 1, n))
    sh_buy = 50 + np.cumsum(rng.normal(0, 1, n))
    sz_buy = 40 + np.cumsum(rng.normal(0, 1, n))
    sh_sell = 45 + np.cumsum(rng.normal(0, 1, n))
    sz_sell = 35 + np.cumsum(rng.normal(0, 1, n))
    sh_turnover = 500 + np.cumsum(rng.normal(0, 5, n))
    sz_turnover = 400 + np.cumsum(rng.normal(0, 5, n))

    ird = usd_swap - cnh_swap
    local_eq = np.log(cnh_eq)
    rate = 7.0 - 0.3 * ird + 0.01 * cnh_cds + 0.3 * local_eq + rng.normal(0, 0.01, n)

    series = {
        "USDCNH_PX_LAST": rate,
        "USD_SWAP": usd_swap,
        "CNH_SWAP": cnh_swap,
        "USD_EQ": usd_eq,
        "CNH_EQ": cnh_eq,
        "CNH_CDS": cnh_cds,
        "MXWO_PX_LAST": mxwo,
        "BRENT_PX_LAST": brent,
        "OFFSHORE_SPREAD_PX_LAST": offshore,
        "ONSHORE_SPREAD_PX_LAST": onshore,
        "SHANGHAI_BUY_FLOWS_PX_LAST": sh_buy,
        "SHENZHEN_BUY_FLOWS_PX_LAST": sz_buy,
        "SHANGHAI_SELL_FLOWS_PX_LAST": sh_sell,
        "SHENZHEN_SELL_FLOWS_PX_LAST": sz_sell,
        "SHANGHAI_FLOWS_TURNOVER_PX_LAST": sh_turnover,
        "SHENZHEN_FLOWS_TURNOVER_PX_LAST": sz_turnover,
    }
    rows = []
    for series_code, vals in series.items():
        for date, value in zip(dates, vals):
            rows.append({"series_code": series_code, "timestamp": date, "value": float(value)})
    return pd.DataFrame(rows)


def _chn_strategy_config() -> StrategyConfig:
    drivers = DRIVER_NAMES + ("offshore_spread", "flows")
    return StrategyConfig(
        universe="CHN",
        window_months=6,
        stop_reward_ratio=1.0,
        logged_rate_threshold=0.0025,
        min_observations=60,
        drivers=drivers,
        expected_signs={name: (0 if name in ("offshore_spread", "flows") else 1) for name in drivers},
    )


@pytest.mark.parametrize(
    "universe,build_metadata,build_values,build_config,series_code",
    [
        ("G10", _unblocked_g10_metadata, _unblocked_g10_values, _g10_strategy_config, "EURUSD_PX_LAST"),
        ("EM", _em_metadata, _em_values, _em_strategy_config, "USDZAR_PX_LAST"),
        ("CHN", _chn_metadata, _chn_values, _chn_strategy_config, "USDCNH_PX_LAST"),
    ],
)
def test_steer_fit_matches_the_asset_pipeline_exactly(
    universe, build_metadata, build_values, build_config, series_code
):
    metadata = build_metadata()
    values = build_values()
    strategy_config = build_config()

    result, data_api = _materialize_asset_pipeline(metadata, values, strategy_config)

    estimate_row = result.output_for_node("steer_estimate", output_name="result").iloc[0]
    signal_row = result.output_for_node("steer_signal", output_name="result").iloc[0]
    cointegration_row = result.output_for_node("steer_cointegration", output_name="result").iloc[0]

    steer = Steer.from_data_api(data_api, universe=universe, strategy_config=strategy_config)
    steer_results = steer.fit(lookback_days=1, cointegration="each")

    fitted = steer_results[series_code]

    # Compare every non-null <driver>_coef the asset wrote -- robust to sign_check_and_reestimate
    # dropping a driver for this particular synthetic dataset (the same drop would happen via
    # either path, since both call the identical function with identical inputs).
    for driver in strategy_config.drivers:
        column = f"{driver}_coef"
        if pd.notna(estimate_row[column]):
            assert driver in fitted.coefficient, f"{driver} missing from Steer's coefficients"
            assert fitted.coefficient[driver] == pytest.approx(estimate_row[column])
        else:
            assert driver not in fitted.coefficient, f"{driver} unexpectedly present (asset dropped it)"
    assert fitted.dropped_variables == tuple(
        filter(None, (estimate_row["dropped_variables"] or "").split(","))
    )
    assert fitted.z_score == pytest.approx(estimate_row["z_score"])
    assert fitted.cointegration_passed == bool(cointegration_row["passed"])

    signals = steer_results.signals()
    fitted_signal = signals.set_index("series_code").loc[series_code]
    assert fitted_signal["signal"] == signal_row["signal"]
    assert fitted_signal["entry_z_score"] == pytest.approx(signal_row["entry_z_score"])
    if pd.notna(signal_row["target"]):
        assert fitted_signal["target"] == pytest.approx(signal_row["target"])
    if pd.notna(signal_row["stop_loss"]):
        assert fitted_signal["stop_loss"] == pytest.approx(signal_row["stop_loss"])


def test_fit_as_of_is_unchanged_by_data_after_it():
    """Acceptance criterion 2: look-ahead safety."""
    metadata = _unblocked_g10_metadata()
    values = _unblocked_g10_values()
    strategy_config = _g10_strategy_config()

    resource = FakeRewriteDataAPIResource(metadata, values)
    steer = Steer.from_data_api(resource.api, universe="G10", strategy_config=strategy_config)

    full_dates = sorted(values["timestamp"].unique())
    cutoff = pd.Timestamp(full_dates[-30])  # a date well before the end of the fetched history

    baseline = steer.fit(as_of=cutoff, lookback_days=1, cointegration="each")

    truncated_values = values[values["timestamp"] <= cutoff]
    truncated_resource = FakeRewriteDataAPIResource(metadata, truncated_values)
    truncated_steer = Steer.from_data_api(
        truncated_resource.api, universe="G10", strategy_config=strategy_config
    )
    truncated = truncated_steer.fit(as_of=cutoff, lookback_days=1, cointegration="each")

    baseline_result = baseline["EURUSD_PX_LAST"]
    truncated_result = truncated["EURUSD_PX_LAST"]

    assert baseline_result.coefficient.equals(truncated_result.coefficient)
    assert baseline_result.z_score == pytest.approx(truncated_result.z_score)
    assert baseline_result.cointegration_passed == truncated_result.cointegration_passed


def test_fit_lookback_days_produces_distinct_dates_with_distinct_coefficients():
    """Acceptance criterion 3."""
    resource = FakeRewriteDataAPIResource(_unblocked_g10_metadata(), _unblocked_g10_values())
    steer = Steer.from_data_api(resource.api, universe="G10", strategy_config=_g10_strategy_config())

    results = steer.fit(lookback_days=5, cointegration="latest")

    assert len(results.as_of_dates) == 5
    coefficients = [
        results.get("EURUSD_PX_LAST", as_of=as_of).coefficient["interest_rate_differential"]
        for as_of in results.as_of_dates
    ]
    assert len(set(coefficients)) > 1  # not all identical -- each date genuinely re-fit


def test_get_cross_section_matches_steer_result_cross_section():
    """Acceptance criterion 4."""
    resource = FakeRewriteDataAPIResource(_unblocked_g10_metadata(), _unblocked_g10_values())
    steer = Steer.from_data_api(resource.api, universe="G10", strategy_config=_g10_strategy_config())

    results = steer.fit(lookback_days=1)
    cross_section = results.get_cross_section(-1)

    assert len(cross_section) == 1
    row = cross_section.iloc[0]
    expected = results["EURUSD_PX_LAST"].cross_section()
    for column in expected.index:
        if column == "as_of":
            continue
        left, right = row[column], expected[column]
        if isinstance(left, float) and isinstance(right, float):
            assert left == pytest.approx(right, nan_ok=True)
        else:
            assert left == right


def test_plot_z_history_raises_a_clear_error_when_lookback_is_one():
    """Acceptance criterion 5."""
    resource = FakeRewriteDataAPIResource(_unblocked_g10_metadata(), _unblocked_g10_values())
    steer = Steer.from_data_api(resource.api, universe="G10", strategy_config=_g10_strategy_config())

    results = steer.fit(lookback_days=1)

    with pytest.raises(ValueError, match="lookback_days"):
        results.plot_z_history()


def test_blocked_pairs_are_skipped_and_recorded():
    resource = FakeRewriteDataAPIResource(*(_unblocked_g10_metadata(), _unblocked_g10_values()))
    # Add a second, unresolvable pair to the metadata so it's discovered but blocked.
    metadata = pd.concat(
        [
            _unblocked_g10_metadata(),
            pd.DataFrame(
                [
                    {"series_code": "GBPUSD_PX_LAST", "asset_class": "Currency",
                     "sub_asset_class": "FX Spot", "market_development": "G10",
                     "currency": "GBP", "tenor": None, "market_segment": None}
                ]
            ),
        ],
        ignore_index=True,
    )
    resource = FakeRewriteDataAPIResource(metadata, _unblocked_g10_values())
    steer = Steer.from_data_api(resource.api, universe="G10", strategy_config=_g10_strategy_config())

    results = steer.fit(lookback_days=1)

    assert "GBPUSD_PX_LAST" in results.blocked
    assert "GBP" in results.blocked["GBPUSD_PX_LAST"]
    assert "EURUSD_PX_LAST" in results.results[results.as_of_dates[-1]]
