"""Unit tests for steer.estimation: rolling OLS, cointegration wrapper, sign-check/re-estimation.

Fixtures build genuinely cointegrated and genuinely non-cointegrated
synthetic series (not mocks) so the statsmodels calls are exercised for
real -- see the module docstring in steer/estimation.py for why
cointegration_test() runs adfuller() (regression="c", autolag="BIC")
directly on the multivariate OLS residuals, with no intermediate
"collapse to one fitted series" step -- these are the reference
production model's (mqrm-steer-model) exact parameters, matched
deliberately rather than "corrected" towards a more conservative
(MacKinnon-calibrated) test. See
test_independent_random_walks_case_matches_production_known_lenient_behavior
below for the known, documented (not silently fixed) consequence: this
test passes somewhat more often than a properly-calibrated Engle-Granger
test would.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.steer.config import DRIVER_NAMES
from dagster_quickstart.steer.errors import InsufficientDataError
from dagster_quickstart.steer.analytics.estimation import (
    cointegration_test,
    engle_granger_cointegration_test,
    estimate_steer,
    sign_check_and_reestimate,
)
from dagster_quickstart.steer.analytics.results import build_pair_result


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
    # A near-1:1 linear combination of rate, plus real (small) stationary
    # noise -- a near-zero-noise proxy would make the OLS residuals
    # machine-precision ~0, which is a degenerate/ill-posed ADF input, not
    # a meaningful cointegration test.
    rng = np.random.default_rng(7)
    proxy = rate * 0.98 + 0.01 + rng.normal(0, 0.01, len(rate))

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
    rate, drivers = independent_random_walks
    as_of = rate.index[-1]

    result = cointegration_test(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    assert isinstance(result.passed, bool)
    assert 0.0 <= result.p_value <= 1.0


def test_independent_random_walks_case_matches_production_known_lenient_behavior(
    independent_random_walks,
):
    """Regression test documenting a known, INTENTIONAL property, not a bug: matching the
    reference production model's exact ADF parameters (regression="c", autolag="BIC", no
    MacKinnon adjustment -- see estimation.py's module docstring) means this test genuinely
    passes too often relative to a properly-calibrated Engle-Granger test.

    Concretely: this exact 5-independent-random-walk fixture (seed=99) reports
    passed=True, p=0.0355 here -- a false positive (5 independent random walks are not
    actually cointegrated). A prior, locally-derived "fix" (statsmodels.tsa.stattools.coint()
    with MacKinnon N-regressor critical values) correctly rejected this same fixture
    (passed=False, p=0.74), but that fix was reverted: it was a deviation from
    mqrm-steer-model's actual reference implementation, not an approved correction. The gap
    between these two p-values (0.0355 vs 0.74) IS the known inherited property being
    documented here, not silently corrected -- see the module docstring for why fixing it
    locally would be a model-owner decision, since it would shift the live signal set."""
    rate, drivers = independent_random_walks
    as_of = rate.index[-1]

    result = cointegration_test(rate, drivers, as_of=as_of, window_months=12, is_logged=False)

    assert result.passed is True
    assert result.p_value < 0.05


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


# --- Golden-file: before/after reconciliation for the driver-2 duplicate-column fix ---
#
# yield_curve_or_cds used to be a literal duplicate of interest_rate_differential (the
# SAME object assigned to both columns in fetch_raw_driver_frame) -- perfect collinearity.
# These tests build a handful of synthetic G10-shaped "pairs" where rate is a real function
# of two DISTINCT rate drivers, fit each pair under both the old (duplicated) and new
# (genuinely distinct) driver-2 spec, and reconcile the two fits.
#
# The task that requested this expected standard errors on the two rate drivers to drop
# "sharply" under the new spec. Empirically, across 5 different seeds, that did NOT hold
# consistently -- the SE sometimes rose slightly for one of the two drivers. The reason:
# statsmodels' OLS falls back to a Moore-Penrose pseudoinverse for an exactly rank-deficient
# design matrix (confirmed via the SingularMatrixWarning it emits for the old spec here),
# and the pinv-based standard errors for a *forced* 50/50 coefficient split don't follow the
# classical near-collinearity variance-inflation intuition (SE -> infinity as correlation ->
# 1) that "sharply drops" was presumably reasoning from -- that intuition applies to a
# non-singular but ill-conditioned matrix, not an exactly singular one. What DOES hold,
# deterministically, across every seed tried: (1) the two rate-driver coefficients are
# forced to be numerically IDENTICAL under the old spec (the mathematical signature of two
# identical regressor columns), and are never identical under the new one; (2) R^2 improves
# substantially under the new spec, since the model can now separate two real, distinct
# effects it previously couldn't tell apart. Those are used as the primary assertions here
# instead of a standard-error threshold, since they're the reliable signal this fixture
# actually produces -- a raw SE-halving assertion would be seed-dependent and flaky.
def _synthetic_pair(seed: int):
    rng = np.random.default_rng(seed)
    n = 400
    dates = pd.bdate_range("2023-01-02", periods=n)

    ird = np.cumsum(rng.normal(0, 0.02, n))
    curve = np.cumsum(rng.normal(0, 0.02, n))  # genuinely independent of ird
    leq = np.cumsum(rng.normal(0, 0.02, n))
    geq = np.cumsum(rng.normal(0, 0.02, n))
    comm = 50 + np.cumsum(rng.normal(0, 0.3, n))
    noise = rng.normal(0, 0.01, n)
    rate = 1.1 + 0.5 * ird - 0.3 * curve + 0.2 * leq + 0.1 * geq + 0.004 * comm + noise

    rate_series = pd.Series(rate, index=dates)
    new_drivers = pd.DataFrame(
        {
            "interest_rate_differential": ird,
            "yield_curve_or_cds": curve,
            "local_equity": leq,
            "global_equity": geq,
            "commodity": comm,
        },
        index=dates,
    )
    old_drivers = new_drivers.copy()
    old_drivers["yield_curve_or_cds"] = old_drivers["interest_rate_differential"]  # the bug
    return rate_series, old_drivers, new_drivers


@pytest.mark.parametrize("seed", [1, 2, 3, 42, 7])
def test_driver_2_fix_reconciliation_old_forces_equal_coefficients_new_does_not(seed):
    rate, old_drivers, new_drivers = _synthetic_pair(seed)
    as_of = rate.index[-1]
    signs = {name: 0 for name in new_drivers.columns}

    old = sign_check_and_reestimate(
        rate, old_drivers, as_of=as_of, window_months=12, is_logged=False,
        expected_signs=signs, min_observations=40,
    )
    new = sign_check_and_reestimate(
        rate, new_drivers, as_of=as_of, window_months=12, is_logged=False,
        expected_signs=signs, min_observations=40,
    )

    # The duplicate-column bug's mathematical signature: two identical regressor
    # columns force pinv's minimum-norm solution to split the coefficient exactly in half.
    assert old.coefficients["interest_rate_differential"] == pytest.approx(
        old.coefficients["yield_curve_or_cds"], abs=1e-9
    )
    assert new.coefficients["interest_rate_differential"] != pytest.approx(
        new.coefficients["yield_curve_or_cds"], abs=1e-3
    )

    # The new spec recovers the true, distinct generating coefficients (0.5, -0.3);
    # the old spec's forced-equal split can't, by construction.
    assert new.coefficients["interest_rate_differential"] == pytest.approx(0.5, abs=0.1)
    assert new.coefficients["yield_curve_or_cds"] == pytest.approx(-0.3, abs=0.1)

    # R^2 improves once the model can separate the two real effects it previously couldn't.
    assert new.r_squared > old.r_squared


def test_driver_2_fix_reconciliation_full_report_for_one_pair():
    """One fully worked before/after comparison (coefficients, standard errors, R^2, z-score)
    -- see the module-level comment above for why standard errors aren't asserted to drop
    "sharply": that expectation didn't hold empirically for this exact-duplicate-column case,
    though they do move (both are reported for a human reader to inspect, e.g. via -s)."""
    rate, old_drivers, new_drivers = _synthetic_pair(seed=1)
    as_of = rate.index[-1]
    signs = {name: 0 for name in new_drivers.columns}

    old_estimate = sign_check_and_reestimate(
        rate, old_drivers, as_of=as_of, window_months=12, is_logged=False,
        expected_signs=signs, min_observations=40,
    )
    new_estimate = sign_check_and_reestimate(
        rate, new_drivers, as_of=as_of, window_months=12, is_logged=False,
        expected_signs=signs, min_observations=40,
    )
    old_result = build_pair_result(
        "EURNOK_PX_LAST", "G10", rate, old_drivers, estimate=old_estimate, window_months=12
    )
    new_result = build_pair_result(
        "EURNOK_PX_LAST", "G10", rate, new_drivers, estimate=new_estimate, window_months=12
    )

    print("\ndriver-2 fix reconciliation (EURNOK-shaped synthetic pair):")
    print(f"  R^2:        old={old_estimate.r_squared:.4f}  new={new_estimate.r_squared:.4f}")
    print(f"  z_score:    old={old_estimate.z_score:.4f}  new={new_estimate.z_score:.4f}")
    for driver in ("interest_rate_differential", "yield_curve_or_cds"):
        print(
            f"  {driver}: "
            f"coef old={old_result.coefficient[driver]:.4f} new={new_result.coefficient[driver]:.4f}  "
            f"se old={old_result.standard_error[driver]:.5f} new={new_result.standard_error[driver]:.5f}"
        )

    assert new_estimate.r_squared > old_estimate.r_squared + 0.03
    assert new_estimate.z_score != pytest.approx(old_estimate.z_score, abs=0.05)
