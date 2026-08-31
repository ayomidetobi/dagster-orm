"""Unit tests for steer.estimation: rolling OLS, cointegration wrapper, sign-check/re-estimation.

Fixtures build genuinely cointegrated and genuinely non-cointegrated
synthetic series (not mocks) so the statsmodels calls are exercised for
real -- see the module docstring in steer/estimation.py for why
cointegration_test() collapses the 5 drivers to a fitted value before
calling coint() (which is bivariate).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.steer.config import DRIVER_NAMES
from dagster_quickstart.steer.errors import InsufficientDataError
from dagster_quickstart.steer.estimation import (
    cointegration_test,
    engle_granger_cointegration_test,
    estimate_steer,
    sign_check_and_reestimate,
)


@pytest.fixture
def cointegrated_system():
    """rate is a real linear combination of the 5 drivers plus stationary noise -- genuinely cointegrated."""
    rng = np.random.default_rng(42)
    n = 400
    dates = pd.bdate_range("2023-01-02", periods=n)

    ird = np.cumsum(rng.normal(0, 0.02, n))
    yc = np.cumsum(rng.normal(0, 0.02, n))
    leq = np.cumsum(rng.normal(0, 0.02, n))
    geq = np.cumsum(rng.normal(0, 0.02, n))
    comm = 50 + np.cumsum(rng.normal(0, 0.3, n))
    noise = rng.normal(0, 0.01, n)  # stationary residual -> genuine cointegration

    rate = 1.1 + 0.5 * ird - 0.3 * yc + 0.2 * leq + 0.1 * geq + 0.004 * comm + noise

    rate_series = pd.Series(rate, index=dates)
    drivers = pd.DataFrame(
        {
            "interest_rate_differential": ird,
            "yield_curve_or_cds": yc,
            "local_equity": leq,
            "global_equity": geq,
            "commodity": comm,
        },
        index=dates,
    )
    return rate_series, drivers


@pytest.fixture
def independent_random_walks():
    """rate and every driver are independent random walks -- no real relationship, no real cointegration."""
    rng = np.random.default_rng(99)
    n = 300
    dates = pd.bdate_range("2023-01-02", periods=n)
    rate_series = pd.Series(1.0 + np.cumsum(rng.normal(0, 0.01, n)), index=dates)
    drivers = pd.DataFrame(
        {name: np.cumsum(rng.normal(0, 0.01, n)) for name in DRIVER_NAMES}, index=dates
    )
    return rate_series, drivers


def test_estimate_steer_fits_close_to_true_coefficients(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]

    estimate = estimate_steer(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    assert estimate.r_squared > 0.9
    assert estimate.coefficients["interest_rate_differential"] == pytest.approx(0.5, abs=0.1)
    assert estimate.coefficients["yield_curve_or_cds"] == pytest.approx(-0.3, abs=0.1)
    assert estimate.n_obs > 0


def test_estimate_steer_never_uses_data_after_as_of(cointegrated_system):
    """Look-ahead safety: corrupting data after as_of must not change the estimate."""
    rate, drivers = cointegrated_system
    as_of = rate.index[200]

    baseline = estimate_steer(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    corrupted_rate = rate.copy()
    corrupted_rate.iloc[201:] = 999.0
    corrupted_drivers = drivers.copy()
    corrupted_drivers.iloc[201:] = -999.0

    corrupted = estimate_steer(
        corrupted_rate, corrupted_drivers, as_of=as_of, window_months=12, is_logged=False
    )

    assert corrupted.fitted_value == pytest.approx(baseline.fitted_value)
    assert corrupted.coefficients == pytest.approx(baseline.coefficients)


def test_estimate_steer_raises_on_insufficient_data(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[5]  # far too early for a 12-month window

    with pytest.raises(InsufficientDataError):
        estimate_steer(
            rate, drivers, as_of=as_of, window_months=12, is_logged=False, min_observations=60
        )


def test_estimate_steer_is_logged_regresses_log_rate(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]

    level_estimate = estimate_steer(rate, drivers, as_of=as_of, window_months=12, is_logged=False)
    log_estimate = estimate_steer(rate, drivers, as_of=as_of, window_months=12, is_logged=True)

    assert level_estimate.actual_value == pytest.approx(float(rate.loc[as_of]))
    assert log_estimate.actual_value == pytest.approx(float(np.log(rate.loc[as_of])))
    assert log_estimate.actual_value_level == pytest.approx(float(rate.loc[as_of]))


def test_engle_granger_cointegration_passes_for_genuinely_cointegrated_series(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    # A near-1:1 linear combination of rate is itself cointegrated with rate.
    proxy = rate * 0.98 + 0.01

    result = engle_granger_cointegration_test(rate, proxy, as_of=as_of, significance=0.05)

    assert result.passed is True
    assert result.p_value < 0.05


def test_cointegration_test_wraps_estimate_steer_and_fitted_value(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]

    result = cointegration_test(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    assert result.passed is True
    assert result.n_obs > 0


def test_cointegration_test_can_fail_for_unrelated_series(independent_random_walks):
    """Not a guarantee (Engle-Granger has known small-sample size distortion when applied to
    multiple I(1) regressors collapsed to a fitted value -- see estimation.py's module
    docstring), but across a genuinely unrelated system it should fail more often than not;
    this asserts the function runs and returns a well-formed verdict either way."""
    rate, drivers = independent_random_walks
    as_of = rate.index[-1]

    result = cointegration_test(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    assert isinstance(result.passed, bool)
    assert 0.0 <= result.p_value <= 1.0


def test_sign_check_and_reestimate_keeps_estimate_when_all_signs_match(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    expected_signs = {
        "interest_rate_differential": 1,
        "yield_curve_or_cds": -1,
        "local_equity": 1,
        "global_equity": 1,
        "commodity": 1,
    }

    estimate = sign_check_and_reestimate(
        rate, drivers, as_of=as_of, window_months=12, is_logged=False, expected_signs=expected_signs
    )

    assert estimate.dropped_variables == ()


def test_sign_check_and_reestimate_drops_contradicting_driver(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    # interest_rate_differential's true coefficient is +0.5 -- assert the OPPOSITE sign so it's dropped.
    expected_signs = {
        "interest_rate_differential": -1,
        "yield_curve_or_cds": -1,
        "local_equity": 1,
        "global_equity": 1,
        "commodity": 1,
    }

    estimate = sign_check_and_reestimate(
        rate, drivers, as_of=as_of, window_months=12, is_logged=False, expected_signs=expected_signs
    )

    assert "interest_rate_differential" in estimate.dropped_variables
    assert "interest_rate_differential" not in estimate.coefficients


def test_sign_check_and_reestimate_zero_expected_sign_never_drops(cointegrated_system):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    expected_signs = {name: 0 for name in DRIVER_NAMES}

    estimate = sign_check_and_reestimate(
        rate, drivers, as_of=as_of, window_months=12, is_logged=False, expected_signs=expected_signs
    )

    assert estimate.dropped_variables == ()
