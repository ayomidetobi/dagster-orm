"""Rolling-window OLS STEER estimation and Engle-Granger cointegration testing.

Pure functions only -- no Dagster, no DuckLake -- so they're directly unit
testable against synthetic series (see tests/test_steer_estimation.py).

Look-ahead safety: every function here takes `as_of` and only ever touches
rows with timestamp <= as_of (see `_window_slice`); nothing past `as_of` is
read, so calling this once per historical day and once "live" today
produces the same result for that day.

Cointegration design note (an edge case flagged rather than guessed):
statsmodels.tsa.stattools.coint is bivariate -- it tests exactly two series
against each other, not a rate against 5 drivers at once. The standard way
to extend Engle-Granger to a multivariate regression is to first collapse
the drivers into the OLS-fitted value (a single series), then run coint()
between the actual rate and that fitted series -- which is what
cointegration_test() below does. engle_granger_cointegration_test() itself
stays a generic bivariate wrapper (any two series in, verdict out) so it's
independently testable and reusable.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Dict, Mapping, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint

from dagster_quickstart.steer.errors import InsufficientDataError

CONST_COLUMN = "const"


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
    passed in, plus CONST_COLUMN -- a driver dropped by
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


def _window_slice(frame: pd.DataFrame, *, as_of: pd.Timestamp, window_months: int) -> pd.DataFrame:
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
    including `as_of` (see _window_slice) -- never anything after `as_of`.

    Raises InsufficientDataError if fewer than min_observations complete
    rows fall in that window (e.g. early in a pair's history, before a full
    window has accumulated).
    """
    y_full = np.log(rate) if is_logged else rate
    frame = pd.concat([y_full.rename("y"), drivers], axis=1)
    windowed = _window_slice(frame, as_of=as_of, window_months=window_months)

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
    residual_std = float(residuals.std(ddof=1))

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
    """Engle-Granger cointegration test between two series (statsmodels.tsa.stattools.coint), as of `as_of`.

    Generic and bivariate -- pass any two series (e.g. actual rate vs. its
    OLS-fitted STEER value; see cointegration_test() below for that
    specific wiring). Only uses rows with timestamp <= as_of.
    """
    aligned = pd.concat([y.rename("y"), x.rename("x")], axis=1)
    aligned = aligned.loc[aligned.index <= pd.Timestamp(as_of)].dropna()

    if len(aligned) < min_observations:
        raise InsufficientDataError(
            f"Only {len(aligned)} overlapping observation(s) as of {as_of} -- "
            f"need at least {min_observations} for a cointegration test."
        )

    test_statistic, p_value, critical_values = coint(aligned["y"], aligned["x"])

    return CointegrationResult(
        as_of=pd.Timestamp(as_of),
        passed=bool(p_value <= significance),
        p_value=float(p_value),
        test_statistic=float(test_statistic),
        critical_values=tuple(float(v) for v in critical_values),
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
    """Cointegration test between a pair's actual rate and its OLS-fitted STEER value, as of `as_of`.

    Fits estimate_steer() over the same rolling window, then runs
    engle_granger_cointegration_test() between the actual (log or level,
    per is_logged) series and the fitted series over that window -- see
    the module docstring for why this is bivariate rather than passing all
    5 drivers to coint() directly.
    """
    y_full = np.log(rate) if is_logged else rate
    frame = pd.concat([y_full.rename("y"), drivers], axis=1)
    windowed = _window_slice(frame, as_of=as_of, window_months=window_months)

    if len(windowed) < max(min_observations, 1):
        raise InsufficientDataError(
            f"Only {len(windowed)} complete observation(s) in the trailing "
            f"{window_months}-month window as of {as_of} -- need at least {min_observations}."
        )

    x = sm.add_constant(windowed[list(drivers.columns)], has_constant="add")
    model = sm.OLS(windowed["y"], x).fit()
    fitted = model.fittedvalues.rename("fitted")

    return engle_granger_cointegration_test(
        windowed["y"],
        fitted,
        as_of=as_of,
        significance=significance,
        min_observations=min_observations,
    )
