"""Unit tests for steer.signals.generate_signal."""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_quickstart.steer.estimation import CointegrationResult, SteerEstimate
from dagster_quickstart.steer.signals import generate_signal

AS_OF = pd.Timestamp("2024-06-03")


def _estimate(*, z_score: float, fitted_value: float, is_logged: bool = False) -> SteerEstimate:
    return SteerEstimate(
        as_of=AS_OF,
        is_logged=is_logged,
        coefficients={"const": 0.0},
        fitted_value=fitted_value,
        actual_value=fitted_value + z_score * 0.01,
        residual_std=0.01,
        z_score=z_score,
        r_squared=0.9,
        n_obs=200,
    )


def _cointegration(*, passed: bool) -> CointegrationResult:
    return CointegrationResult(
        as_of=AS_OF,
        passed=passed,
        p_value=0.01 if passed else 0.5,
        test_statistic=-3.0,
        critical_values=(0.0, 0.0, 0.0),
        n_obs=200,
    )


def test_no_signal_when_cointegration_fails_despite_large_z():
    estimate = _estimate(z_score=5.0, fitted_value=1.10)
    cointegration = _cointegration(passed=False)

    signal = generate_signal(
        estimate, cointegration, current_rate=1.15, z_threshold=1.5, stop_reward_ratio=2.0
    )

    assert signal.signal == "NONE"
    assert "cointegration" in signal.reason
    assert signal.target is None
    assert signal.stop_loss is None


def test_no_signal_when_z_below_threshold():
    estimate = _estimate(z_score=0.5, fitted_value=1.10)
    cointegration = _cointegration(passed=True)

    signal = generate_signal(
        estimate, cointegration, current_rate=1.105, z_threshold=1.5, stop_reward_ratio=2.0
    )

    assert signal.signal == "NONE"
    assert "threshold" in signal.reason


def test_sell_signal_when_rate_above_fair_value():
    estimate = _estimate(z_score=2.0, fitted_value=1.10)
    cointegration = _cointegration(passed=True)
    current_rate = 1.15

    signal = generate_signal(
        estimate, cointegration, current_rate=current_rate, z_threshold=1.5, stop_reward_ratio=2.0
    )

    assert signal.signal == "SELL"
    assert signal.target == pytest.approx(1.10)
    # reward = |1.15 - 1.10| = 0.05, risk = 0.05/2 = 0.025, stop = current + risk (against the short)
    assert signal.stop_loss == pytest.approx(1.15 + 0.025)


def test_buy_signal_when_rate_below_fair_value():
    estimate = _estimate(z_score=-2.0, fitted_value=1.10)
    cointegration = _cointegration(passed=True)
    current_rate = 1.05

    signal = generate_signal(
        estimate, cointegration, current_rate=current_rate, z_threshold=1.5, stop_reward_ratio=2.0
    )

    assert signal.signal == "BUY"
    assert signal.target == pytest.approx(1.10)
    # reward = |1.05 - 1.10| = 0.05, risk = 0.05/2 = 0.025, stop = current - risk (against the long)
    assert signal.stop_loss == pytest.approx(1.05 - 0.025)


def test_tighter_stop_reward_ratio_widens_the_stop():
    estimate = _estimate(z_score=2.0, fitted_value=1.10)
    cointegration = _cointegration(passed=True)
    current_rate = 1.15

    two_to_one = generate_signal(
        estimate, cointegration, current_rate=current_rate, z_threshold=1.5, stop_reward_ratio=2.0
    )
    one_to_one = generate_signal(
        estimate, cointegration, current_rate=current_rate, z_threshold=1.5, stop_reward_ratio=1.0
    )

    # Lower ratio -> risk = reward / ratio grows -> stop further from current.
    assert abs(one_to_one.stop_loss - current_rate) > abs(two_to_one.stop_loss - current_rate)


def test_logged_estimate_converts_target_back_to_rate_level():
    import numpy as np

    fitted_log_value = np.log(1.10)
    estimate = _estimate(z_score=2.0, fitted_value=fitted_log_value, is_logged=True)
    cointegration = _cointegration(passed=True)

    signal = generate_signal(
        estimate, cointegration, current_rate=1.15, z_threshold=1.5, stop_reward_ratio=2.0
    )

    assert signal.target == pytest.approx(1.10, abs=1e-6)
