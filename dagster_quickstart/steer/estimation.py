"""Rolling-window OLS STEER estimation and Engle-Granger cointegration testing.

Pure functions only -- no Dagster, no DuckLake -- so they're directly unit
testable against synthetic series (see tests/test_steer_estimation.py).

Look-ahead safety: every function here takes `as_of` and only ever touches
rows with timestamp <= as_of (see `window_slice`); nothing past `as_of` is
read, so calling this once per historical day and once "live" today
produces the same result for that day.

Cointegration design note (matches the reference production model, not a
locally-derived "fix"): cointegration_test() runs statsmodels.tsa.stattools.
adfuller() directly on the residuals of the multivariate OLS of rate on
every driver, with regression="c" and autolag="BIC" -- these are the exact
parameters BNP Paribas' production STEER model (mqrm-steer-model) uses:
`stats.adfuller(residuals[pair], regression="c", autolag="BIC")`. There is
no intermediate "collapse the drivers to one OLS-fitted series, then
regress the actual rate on THAT" step -- that step was already a
mathematical no-op (regressing y on its own fitted values from an
identical design gives slope=1, intercept=0, and residuals identical to
the original multivariate regression's), and production's use of adfuller
directly on the multivariate residuals confirms the no-op step was never
needed, not that it needs replacing with something else.

Known inherited property, documented rather than silently corrected: ADF's
own critical values are calibrated for testing an observed series, not the
residuals of an estimated regression -- properly calibrated Engle-Granger
critical values (e.g. via MacKinnon's tables, as statsmodels.tsa.stattools.
coint() applies) would be somewhat more conservative, so this test passes
somewhat more often than a properly-calibrated one would. That gap is a
known, accepted property of the reference implementation this codebase is
matching, not a bug to fix locally -- changing it would shift the live
signal set relative to production and is a model-owner decision, not an
engineering one.

engle_granger_cointegration_test() stays a separate, reusable bivariate
(two-series) OLS-then-adfuller wrapper for callers that want a plain
two-series test (see tests/test_steer_estimation.py) -- cointegration_test()
no longer routes through it, since routing a multivariate fit through a
bivariate collapse was exactly the no-op step above.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Dict, Mapping, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

from dagster_quickstart.steer.constants import (
    ADF_AUTOLAG_CRITERION,
    ADF_CRITICAL_VALUE_LEVELS,
    ADF_REGRESSION_TYPE,
)
from dagster_quickstart.steer.errors import InsufficientDataError


@dataclass(frozen=True)
class CointegrationResult:
    """Result of an Engle-Granger cointegration test between two series."""

    as_of: pd.Timestamp
    passed: bool
    p_value: float
    test_statistic: float
    critical_values: Tuple[float, ...]
    n_obs: int


@dataclass(frozen=True)
class SteerEstimate:
    """One rolling-OLS STEER fit for one currency pair, as of one day.

    `coefficients` always has one entry per column of the `drivers` frame
    passed in, plus "const" (statsmodels' constant-column name) -- a driver dropped by
    sign_check_and_reestimate() simply isn't a key here (rather than being
    present with a null/zero value), so `dropped_variables` is the only
    place that fact is recorded.
    """

    as_of: pd.Timestamp
    is_logged: bool
    coefficients: Dict[str, float]
    fitted_value: float
    actual_value: float
    residual_std: float
    z_score: float
    r_squared: float
    n_obs: int
    dropped_variables: Tuple[str, ...] = ()

    @property
    def fitted_value_level(self) -> float:
        """fitted_value converted back to a real rate level (undoes log() if is_logged)."""
        return float(np.exp(self.fitted_value)) if self.is_logged else self.fitted_value

    @property
    def actual_value_level(self) -> float:
        """actual_value converted back to a real rate level (undoes log() if is_logged)."""
        return float(np.exp(self.actual_value)) if self.is_logged else self.actual_value


def window_slice(frame: pd.DataFrame, *, as_of: pd.Timestamp, window_months: int) -> pd.DataFrame:
    """Rows with window_start < timestamp <= as_of, dropping any row with a null anywhere."""
    as_of = pd.Timestamp(as_of)
    window_start = as_of - pd.DateOffset(months=window_months)
    return frame.loc[(frame.index > window_start) & (frame.index <= as_of)].dropna()


def estimate_steer(
    rate: pd.Series,
    drivers: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    window_months: int,
    is_logged: bool,
    min_observations: int = 40,
) -> SteerEstimate:
    """Rolling-window OLS of `rate` on `drivers`, evaluated as of `as_of`.

    is_logged decides whether the regression's dependent variable is
    log(rate) or the raw rate level -- see steer.features.should_use_logged_rate
    for how that's decided per pair/day; this function just applies whatever
    it's told. Uses only the trailing `window_months` of history up to and
    including `as_of` (see window_slice) -- never anything after `as_of`.

    Raises InsufficientDataError if fewer than min_observations complete
    rows fall in that window (e.g. early in a pair's history, before a full
    window has accumulated).
    """
    y_full = np.log(rate) if is_logged else rate
    frame = pd.concat([y_full.rename("y"), drivers], axis=1)
    windowed = window_slice(frame, as_of=as_of, window_months=window_months)

    if len(windowed) < min_observations:
        raise InsufficientDataError(
            f"Only {len(windowed)} complete observation(s) in the trailing "
            f"{window_months}-month window as of {as_of} -- need at least "
            f"{min_observations}."
        )

    y = windowed["y"]
    x = sm.add_constant(windowed[list(drivers.columns)], has_constant="add")
    model = sm.OLS(y, x).fit()

    fitted = model.fittedvalues
    residuals = model.resid
    # ddof=0 (population, not sample, std) -- matches the reference production
    # model; this feeds z_score directly, and the residual_std/ddof choice
    # lands on the +/-1.5 threshold where signals fire (see steer.signals).
    residual_std = float(residuals.std(ddof=0))

    latest_actual = float(y.iloc[-1])
    latest_fitted = float(fitted.iloc[-1])
    z_score = (latest_actual - latest_fitted) / residual_std if residual_std else 0.0

    return SteerEstimate(
        as_of=pd.Timestamp(as_of),
        is_logged=is_logged,
        coefficients={str(k): float(v) for k, v in model.params.items()},
        fitted_value=latest_fitted,
        actual_value=latest_actual,
        residual_std=residual_std,
        z_score=float(z_score),
        r_squared=float(model.rsquared),
        n_obs=len(windowed),
    )


def sign_check_and_reestimate(
    rate: pd.Series,
    drivers: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    window_months: int,
    is_logged: bool,
    expected_signs: Mapping[str, int],
    min_observations: int = 40,
) -> SteerEstimate:
    """estimate_steer(), then drop any driver whose fitted sign contradicts economic intuition and re-run once.

    expected_signs maps driver name -> +1/-1/0 (0 means "no expectation,
    never drop"). A driver is dropped if its coefficient's sign disagrees
    with its expected sign (coefficient * expected_sign < 0). Re-runs the
    regression at most once with the offending driver(s) removed -- not an
    iterative loop -- matching "drop that variable and re-run the
    regression" rather than repeatedly re-checking the new fit's signs too.
    Returns the original (unmodified) estimate if nothing was dropped, or
    the re-estimated one with `dropped_variables` populated.
    """
    estimate = estimate_steer(
        rate,
        drivers,
        as_of=as_of,
        window_months=window_months,
        is_logged=is_logged,
        min_observations=min_observations,
    )

    dropped = tuple(
        driver_name
        for driver_name, expected_sign in expected_signs.items()
        if expected_sign != 0
        and driver_name in estimate.coefficients
        and estimate.coefficients[driver_name] * expected_sign < 0
    )
    if not dropped:
        return estimate

    kept_columns = [column for column in drivers.columns if column not in dropped]
    if not kept_columns:
        raise InsufficientDataError(
            f"Every driver's sign contradicted expected_signs as of {as_of} "
            f"(dropped {dropped}) -- nothing left to regress on."
        )

    re_estimate = estimate_steer(
        rate,
        drivers[kept_columns],
        as_of=as_of,
        window_months=window_months,
        is_logged=is_logged,
        min_observations=min_observations,
    )
    return replace(re_estimate, dropped_variables=dropped)


def engle_granger_cointegration_test(
    y: pd.Series,
    x: pd.Series,
    *,
    as_of: pd.Timestamp,
    significance: float = 0.05,
    min_observations: int = 20,
) -> CointegrationResult:
    """Engle-Granger cointegration test between two series: OLS regression, then ADF on the residuals.

    Generic and bivariate -- pass any two series. Only uses rows with
    timestamp <= as_of. cointegration_test() below does NOT route through
    this (see the module docstring) -- it runs ADF directly on the
    multivariate regression's own residuals instead, matching production.
    This function remains for callers that genuinely want a plain
    two-series test.

    (1) OLS of y on x with a constant; (2) statsmodels.tsa.stattools.
    adfuller() on the regression residuals, with regression="c" and
    autolag="BIC" -- the reference production model's exact parameters
    (see module docstring for why, and for the known critical-value
    caveat this inherits). `passed` is True when adfuller rejects the
    unit-root null (p_value <= significance) -- i.e. the residuals are
    stationary, so y and x are cointegrated.
    """
    aligned = pd.concat([y.rename("y"), x.rename("x")], axis=1)
    aligned = aligned.loc[aligned.index <= pd.Timestamp(as_of)].dropna()

    if len(aligned) < min_observations:
        raise InsufficientDataError(
            f"Only {len(aligned)} overlapping observation(s) as of {as_of} -- "
            f"need at least {min_observations} for a cointegration test."
        )

    x_with_const = sm.add_constant(aligned["x"], has_constant="add")
    residuals = sm.OLS(aligned["y"], x_with_const).fit().resid

    adf_result = adfuller(
        residuals, regression=ADF_REGRESSION_TYPE, autolag=ADF_AUTOLAG_CRITERION, result_object=True
    )

    return CointegrationResult(
        as_of=pd.Timestamp(as_of),
        passed=bool(adf_result.pvalue <= significance),
        p_value=float(adf_result.pvalue),
        test_statistic=float(adf_result.statistic),
        critical_values=tuple(
            float(adf_result.critical_values[level]) for level in ADF_CRITICAL_VALUE_LEVELS
        ),
        n_obs=len(aligned),
    )


def cointegration_test(
    rate: pd.Series,
    drivers: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    window_months: int,
    is_logged: bool,
    significance: float = 0.05,
    min_observations: int = 20,
) -> CointegrationResult:
    """Cointegration test between a pair's actual rate and its drivers, as of `as_of`.

    Fits the identical windowed multivariate OLS estimate_steer() does
    (rate on every driver, plus a constant) and runs adfuller() directly on
    THAT regression's own residuals -- regression="c", autolag="BIC",
    matching the reference production model exactly (see module
    docstring). No intermediate bivariate collapse: that step was
    mathematically a no-op, and production's direct-on-multivariate-residuals
    approach confirms it.
    """
    y_full = np.log(rate) if is_logged else rate
    frame = pd.concat([y_full.rename("y"), drivers], axis=1)
    windowed = window_slice(frame, as_of=as_of, window_months=window_months)

    if len(windowed) < max(min_observations, 1):
        raise InsufficientDataError(
            f"Only {len(windowed)} complete observation(s) in the trailing "
            f"{window_months}-month window as of {as_of} -- need at least {min_observations}."
        )

    x = sm.add_constant(windowed[list(drivers.columns)], has_constant="add")
    residuals = sm.OLS(windowed["y"], x).fit().resid

    adf_result = adfuller(
        residuals, regression=ADF_REGRESSION_TYPE, autolag=ADF_AUTOLAG_CRITERION, result_object=True
    )

    return CointegrationResult(
        as_of=pd.Timestamp(as_of),
        passed=bool(adf_result.pvalue <= significance),
        p_value=float(adf_result.pvalue),
        test_statistic=float(adf_result.statistic),
        critical_values=tuple(
            float(adf_result.critical_values[level]) for level in ADF_CRITICAL_VALUE_LEVELS
        ),
        n_obs=len(windowed),
    )
