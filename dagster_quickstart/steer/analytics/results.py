"""PairResult: one consolidated, retrievable artifact per currency pair, and the pandera
schemas for the three STEER table boundaries (features, estimates, signals) it and its
upstream feature table are validated against.

Bundles a pair's spot rate, this variant's driver series (whatever
StrategyConfig.drivers holds for it -- 5 for G10/EM, 7 for CHN), the
regression's design/response/fitted/residual, and its statistical
diagnostics (coefficients, standard errors, p-values, z-score,
upper_bound/lower_bound) into one object per pair -- everything already
computed elsewhere in steer/ (source/features.py, analytics/estimation.py) via
build_pair_result(), just packaged for easy per-pair retrieval
(PairResult.save()/.load()), cross-pair comparison (cross_section()), and
plotting (plot()).

upper_bound/lower_bound = fitted +/- z_threshold * residual_std -- the SAME boundary
generate_signal (steer/analytics/estimation.py) uses to decide whether a BUY/SELL fires
(|z_score| >= z_threshold), matching the reference production model exactly (Figs 16/17 of the
published methodology shade this exact band). This is deliberately NOT a statistical confidence
interval on the fitted mean (which measures parameter uncertainty, not residual dispersion, and
on a ~250-observation window is roughly 5x narrower than this trigger band) -- PairResult.plot()
must shade the actual trading-trigger band, not something with no relationship to where signals
fire.

Drivers are held as a `drivers: dict[str, pd.Series]` (driver name -> series), not fixed
dataclass fields, so CHN's 2 extra drivers (offshore_spread, flows) don't need a per-variant
subclass or None-padding. `design` derives the regression's X matrix (drivers + a constant)
from this dict on demand.

Every time-series field (spot, every entry of `drivers`, response, fitted,
residual, upper_bound, lower_bound) shares the identical DatetimeIndex --
the trailing regression window ending at `as_of` (see
window_slice, steer/analytics/estimation.py) -- so they concatenate/plot directly with
no reindexing.

build_pair_result() takes an already-computed SteerEstimate (from sign_check_and_reestimate)
rather than independently re-fitting and re-deriving z_score/dropped_variables itself, so the
two can never silently disagree about which drivers were dropped or what the z_score is --
this module still fits its own OLS (over the identical window, on the identical kept-driver
columns) to get the *full* windowed design/response/fitted/residual series plus per-coefficient
p-values/standard errors and the fitted +/- z_threshold * residual_std trigger band that
SteerEstimate doesn't expose (only its latest-day z_score) -- residual_std uses ddof=0, matching
estimate_steer's z_score calculation exactly (see steer/analytics/estimation.py's module docstring).

markov_state is a reserved field, always None today -- no Markov
regime-switching model exists anywhere in this codebase; it's a
placeholder for a future addition, not a fabricated value.

PairResult.save()/.load() are thin delegates onto steer.orm.save_result()/.load_result() --
see steer/orm.py's module docstring for why the import has to live inside the method body
rather than at module level (this module sits below orm.py in steer/'s import direction, so a
module-level import here would invert that). Persisted into 2 DuckLake gold tables --
gold.steer_results (long-form, one row per series_code/as_of/date, the time-series fields) and
gold.steer_result_summary (one row per series_code/as_of, the scalar/cross-sectional fields) --
the same mechanism gold.steer_estimates/gold.steer_signals already use, and the same DuckLake
connection the rest of a run's DataAPI calls share (no second attach). Both tables are
append-only: a re-run for the same pair/as_of writes a new snapshot rather than overwriting --
load() returns the most recent one by default. Different variants writing different driver
sets to the same table is exactly what DataAPI.write_table()'s column-widening handles -- see
that method's docstring (GenericTableRepository.write(), in the DuckLake data-access package).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pandera as pa
import statsmodels.api as sm

from dagster_quickstart.steer.analytics.estimation import (
    CointegrationResult,
    SteerEstimate,
    SteerSignal,
    window_slice,
)
from dagster_quickstart.steer.constants import (
    DRIVER_NAMES,
    IS_LOGGED_COLUMN,
    RATE_COLUMN,
    REALIZED_VOLATILITY_COLUMN,
    SIGNAL_BUY,
    SIGNAL_NONE,
    SIGNAL_SELL,
    VARIANTS,
)

#: to_frame()'s fixed (non-driver) time-series columns -- everything else
#: in a loaded gold.steer_results row is a driver series (see steer.orm.load_result()).
#: fair_value is included here (not a dataclass field -- it's derived from
#: fitted/is_logged via the fair_value property) purely so load_result()'s driver
#: inference doesn't mistake it for a driver column.
_FIXED_TIMESERIES_COLUMNS = (
    "spot",
    "response",
    "fitted",
    "residual",
    "upper_bound",
    "lower_bound",
    "fair_value",
)


def flatten_by_driver(prefix: str, series: pd.Series) -> Dict[str, Any]:
    """A driver-keyed Series (driver name -> value) as `{prefix}{driver}` -> value entries.

    The single place a driver-indexed Series becomes flat, name-suffixed columns --
    cross_section() calls this three times (coefficient_/standard_error_/p_value_), and
    unflatten_by_driver (below) is its exact inverse, used by steer.orm.load_result() to parse
    those same columns back. Kept together so a rename of one side can't silently drift from
    the other, the way three independent f-strings/manual slices could.
    """
    return {f"{prefix}{name}": value for name, value in series.items()}


def unflatten_by_driver(prefix: str, row: pd.Series) -> pd.Series:
    """The inverse of flatten_by_driver: every `row` entry whose key starts with `prefix`,
    keyed back by driver name (the part after `prefix`) instead of the flat column name.

    Drops any value that's NaN -- padding from another variant's wider driver set sharing the
    same physical table (see GenericTableRepository.write()'s column widening), not a real
    coefficient/standard-error/p-value for this pair.
    """
    return pd.Series(
        {
            str(key)[len(prefix) :]: value
            for key, value in row.items()
            if str(key).startswith(prefix) and pd.notna(value)
        }
    )


@dataclass(frozen=True)
class PairResult:
    """One pair's full STEER snapshot -- prices, drivers, regression diagnostics.

    fx is an alias for spot (same series, both names are common FX
    terminology). design is derived on demand from `drivers` (+ a constant
    column) rather than stored twice.

    upper/lower and upper_bound/lower_bound are the same KIND of object --
    fitted +/- z_threshold * residual_std, the reference production model's
    actual trading-trigger boundary (see module docstring) -- they differ
    only in shape: upper_bound/lower_bound are the full windowed *series*
    (the band drawn on a chart), while upper/lower are a single scalar
    *level*. upper/lower are now properties derived from `signal`
    (generate_signal's own Signal.target/.stop_loss, for the signal
    actually generated for this pair/as_of) rather than stored fields of
    their own -- `signal` holds the whole SteerSignal (BUY/SELL/NONE +
    entry_z_score + reason), not just the two price levels PairResult used
    to keep, so a pair's trading signal is now part of what PairResult IS,
    not a fact tracked separately alongside it (see SteerResults.signals(),
    steer/model.py). signal is optional: a pair that hasn't had a signal
    generated yet (e.g. cointegration failed before generate_signal ran)
    still gets a full PairResult, just with signal (and so upper/lower)
    left None.
    """

    series_code: str
    variant: str
    as_of: pd.Timestamp
    is_logged: bool

    spot: pd.Series
    #: driver name -> series (this variant's StrategyConfig.drivers set).
    drivers: Dict[str, pd.Series]

    response: pd.Series
    fitted: pd.Series
    residual: pd.Series
    upper_bound: pd.Series
    lower_bound: pd.Series

    coefficient: pd.Series
    standard_error: pd.Series
    p_values: pd.Series

    z_score: float
    #: Drivers sign_check_and_reestimate dropped before this fit -- see
    #: build_pair_result's docstring for why this now always agrees with
    #: the SteerEstimate it was built from.
    dropped_variables: Tuple[str, ...] = ()
    cointegration_passed: Optional[bool] = None
    #: generate_signal's own result for this pair/as_of, if one was generated -- see this
    #: class's own docstring for why upper/lower are now derived from it rather than stored
    #: fields of their own.
    signal: Optional[SteerSignal] = None
    markov_state: Optional[str] = None

    @property
    def upper(self) -> Optional[float]:
        """The signal's target price level, if a signal was generated -- see class docstring."""
        return self.signal.target if self.signal else None

    @property
    def lower(self) -> Optional[float]:
        """The signal's stop-loss price level, if a signal was generated -- see class docstring."""
        return self.signal.stop_loss if self.signal else None

    @property
    def fx(self) -> pd.Series:
        """Alias for spot -- same series, FX terminology."""
        return self.spot

    @property
    def fair_value(self) -> pd.Series:
        """Fitted STEER value as a rate level (undoes log() if is_logged) -- comparable to `spot`.

        `fitted` lives in regression space: log(rate) when is_logged, the
        raw rate otherwise -- same regression-space/level-space split as
        SteerEstimate.fitted_value/.fitted_value_level. Without this,
        plot() drew `spot` (~7.2 for a logged pair) against `fitted`
        (~1.97, its log) on the same axes.
        """
        return np.exp(self.fitted) if self.is_logged else self.fitted

    @property
    def fair_value_upper(self) -> pd.Series:
        """upper_bound as a rate level -- see fair_value. exp() of a symmetric log-space band is
        asymmetric around fair_value; that's the correct log-normal shape, not something to
        symmetrise or recentre."""
        return np.exp(self.upper_bound) if self.is_logged else self.upper_bound

    @property
    def fair_value_lower(self) -> pd.Series:
        """lower_bound as a rate level -- see fair_value_upper."""
        return np.exp(self.lower_bound) if self.is_logged else self.lower_bound

    @property
    def design(self) -> pd.DataFrame:
        """The regression's X matrix (kept drivers + a constant column), rebuilt on demand.

        Excludes dropped_variables -- `drivers` holds every driver's raw
        series for display/reporting even when sign_check_and_reestimate
        dropped it, but the actual regression (and this X matrix) never
        included it.
        """
        kept = {name: series for name, series in self.drivers.items() if name not in self.dropped_variables}
        return sm.add_constant(pd.DataFrame(kept), has_constant="add")

    def to_frame(self) -> pd.DataFrame:
        """Every time-series field as a column, one row per date -- the natural shape for export or plotting.

        fair_value is included alongside fitted (equal when is_logged is
        False, both useful when it's True -- see the fair_value property).
        """
        return pd.DataFrame(
            {
                "spot": self.spot,
                **self.drivers,
                "response": self.response,
                "fitted": self.fitted,
                "fair_value": self.fair_value,
                "residual": self.residual,
                "upper_bound": self.upper_bound,
                "lower_bound": self.lower_bound,
            }
        )

    def cross_section(self) -> pd.Series:
        """This pair's scalar summary as one flat row (series_code, z_score, fair_value, upper/lower,
        signal/reason, dropped_variables, cointegration_passed, and every coefficient/standard_error/
        p_value with a `{name}_<driver>` suffix). Concat several pairs' cross_section() rows (e.g.
        `pd.DataFrame([r.cross_section() for r in results])`) to compare pairs side by side at one
        point in time -- the econometric sense of "cross section", as opposed to one pair's own time
        series. fair_value is the latest rate-level fitted value -- the number anyone comparing pairs
        side by side actually wants (see the fair_value property). signal/reason (None/None if no
        signal was generated) are what makes this round-trip through save()/load() -- see this
        class's own docstring and SteerResults.signals().
        """
        row: dict = {
            "series_code": self.series_code,
            "variant": self.variant,
            "as_of": self.as_of,
            "is_logged": self.is_logged,
            "z_score": self.z_score,
            "fair_value": float(self.fair_value.iloc[-1]),
            "dropped_variables": ",".join(self.dropped_variables),
            "cointegration_passed": self.cointegration_passed,
            "upper": self.upper,
            "lower": self.lower,
            "signal": self.signal.signal if self.signal else None,
            "reason": self.signal.reason if self.signal else None,
            "markov_state": self.markov_state,
        }
        row.update(flatten_by_driver("coefficient_", self.coefficient))
        row.update(flatten_by_driver("standard_error_", self.standard_error))
        row.update(flatten_by_driver("p_value_", self.p_values))
        return pd.Series(row)

    def plot(self, *, ax=None):
        """Matplotlib chart: spot vs. fair value, with the +/-z_threshold trigger band shaded.

        Every series/line here is a rate LEVEL -- spot, fair_value (not
        fitted), fair_value_upper/fair_value_lower (not upper_bound/
        lower_bound), and target/stop -- never a log-space series. For a
        logged pair, `fitted` is ~log(rate) (e.g. ~1.97 for a spot ~7.2);
        plotting `fitted` directly against `spot` put two different units
        on the same axes.

        target/stop-loss (upper/lower) are colored by role -- target always
        green, stop-loss always red -- never by which one happens to be the
        numerically larger price. For a SELL signal the target sits BELOW
        the current rate and the stop ABOVE it (generate_signal);
        that's the correct trade geometry, not a rendering bug -- coloring
        by numeric position instead of role would be the actual bug (it'd
        mislabel the stop as the target for every SELL).

        Returns the Axes (creates a new figure if `ax` isn't given).
        matplotlib is imported lazily so it stays an optional dependency
        for callers that never plot.
        """
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=(10, 5))

        ax.plot(self.spot.index, self.spot, label="spot", color="black")
        ax.plot(self.fair_value.index, self.fair_value, label="fair value (STEER)", color="tab:blue")
        ax.fill_between(
            self.fair_value_upper.index,
            self.fair_value_lower,
            self.fair_value_upper,
            color="tab:blue",
            alpha=0.15,
            label="+/-z_threshold band",
        )
        if self.upper is not None:
            ax.axhline(self.upper, color="tab:green", linestyle="--", label="target")
        if self.lower is not None:
            ax.axhline(self.lower, color="tab:red", linestyle="--", label="stop")
        ax.set_title(f"{self.series_code} ({self.variant}) -- z={self.z_score:.2f}")
        ax.legend()
        return ax

    def save(self, data_api: Any) -> None:
        """Thin delegate to steer.orm.save_result() -- see steer/orm.py's module docstring for
        why the import has to be inside the method body rather than at module level."""
        from dagster_quickstart.steer.orm import save_result

        save_result(self, data_api)

    @classmethod
    def load(
        cls, data_api: Any, series_code: str, *, as_of: Optional[pd.Timestamp] = None
    ) -> "PairResult":
        """Thin delegate to steer.orm.load_result() -- see save()'s docstring for why the
        import is inside the method body."""
        from dagster_quickstart.steer.orm import load_result

        return load_result(data_api, series_code, as_of=as_of)


def build_pair_result(
    series_code: str,
    variant: str,
    rate: pd.Series,
    drivers: pd.DataFrame,
    *,
    estimate: SteerEstimate,
    window_months: int,
    z_threshold: float = 1.5,
    cointegration: Optional[CointegrationResult] = None,
    signal: Optional[SteerSignal] = None,
) -> PairResult:
    """Build one pair's PairResult from its raw rate + driver frame and an already-computed SteerEstimate.

    `drivers` has this pair's variant's driver columns (StrategyConfig.drivers
    -- 5 for G10/EM, 7 for CHN); `estimate` is
    sign_check_and_reestimate's result for the identical
    rate/drivers/as_of/window_months -- see the module docstring for why
    this takes the estimate as input rather than independently re-fitting:
    z_score/dropped_variables now always agree with it by construction.
    Uses `estimate.as_of`/`estimate.is_logged`/`estimate.dropped_variables`
    to reproduce the identical fit (same window, same kept columns) but
    keeps the full windowed design/response/fitted/residual series (not
    just estimate's latest-day values) and adds per-coefficient
    p-values/standard errors plus upper_bound/lower_bound = fitted +/-
    `z_threshold` * residual_std -- the reference production model's actual
    trading-trigger band, not a statistical confidence interval (see
    module docstring). `z_threshold` should come from this pair's
    StrategyConfig.z_threshold (default 1.5 matches StrategyConfig's own
    default) so the shaded band always matches whatever threshold
    generate_signal actually used to decide BUY/SELL/NONE.

    cointegration (cointegration_test's result for this
    pair/as_of, if available) becomes cointegration_passed.
    signal (generate_signal's result for this pair/as_of, if one was
    already generated) becomes this PairResult's own signal -- upper/lower
    are then derived from it (see PairResult's docstring); pass None to
    leave signal/upper/lower all unset. signal.as_of/.entry_z_score are
    asserted to agree with estimate.as_of/.z_score -- they're now
    duplicated data (computed in different places, by generate_signal and
    sign_check_and_reestimate respectively), and a mismatch would mean a
    real upstream bug, not something to relax this check for.
    """
    rate_f = rate.astype(float)
    drivers_f = drivers.astype(float)
    as_of = estimate.as_of
    is_logged = estimate.is_logged

    kept_columns = [column for column in drivers_f.columns if column not in estimate.dropped_variables]

    y_full = rate_f.transform("log") if is_logged else rate_f
    frame = pd.concat([y_full.rename("y"), drivers_f[kept_columns]], axis=1)
    windowed = window_slice(frame, as_of=as_of, window_months=window_months)

    response = windowed["y"]
    design = sm.add_constant(windowed[kept_columns], has_constant="add")
    model = sm.OLS(response, design).fit()

    fitted = model.fittedvalues
    residual = model.resid
    # ddof=0 -- matches estimate_steer's residual_std/z_score calculation
    # exactly (see steer/analytics/estimation.py's module docstring), so this band and
    # SteerEstimate.z_score agree on what "one residual_std" means.
    residual_std = float(residual.std(ddof=0))

    driver_series = {name: drivers_f[name].reindex(windowed.index) for name in drivers_f.columns}

    if signal is not None:
        assert pd.Timestamp(signal.as_of) == pd.Timestamp(as_of), (
            f"signal.as_of ({signal.as_of}) disagrees with estimate.as_of ({as_of}) for "
            f"{series_code!r} -- generate_signal and sign_check_and_reestimate should always "
            "be called for the identical as_of; a mismatch means a real bug upstream, not "
            "something for this assertion to relax."
        )
        assert signal.entry_z_score == estimate.z_score, (
            f"signal.entry_z_score ({signal.entry_z_score}) disagrees with estimate.z_score "
            f"({estimate.z_score}) for {series_code!r} -- generate_signal takes estimate.z_score "
            "directly, so these should always be identical; a mismatch means a real bug "
            "upstream, not something for this assertion to relax."
        )

    return PairResult(
        series_code=series_code,
        variant=variant,
        as_of=pd.Timestamp(as_of),
        is_logged=is_logged,
        spot=rate_f.reindex(windowed.index),
        drivers=driver_series,
        response=response,
        fitted=fitted,
        residual=residual,
        upper_bound=fitted + z_threshold * residual_std,
        lower_bound=fitted - z_threshold * residual_std,
        coefficient=model.params,
        standard_error=model.bse,
        p_values=model.pvalues,
        z_score=estimate.z_score,
        dropped_variables=estimate.dropped_variables,
        cointegration_passed=cointegration.passed if cointegration is not None else None,
        signal=signal,
        markov_state=None,
    )


#: Plausible bounds for an FX spot rate -- wide enough to cover every G10/EM
#: pair (from ~0.001 JPY-style quote conventions up to ~2000 for some EM
#: pairs quoted in local-currency-per-USD) without being a no-op check.
_RATE_MIN, _RATE_MAX = 1e-4, 1e5

#: A coefficient/z-score/fitted-value this large means something upstream
#: broke (bad units, a divide-by-near-zero) -- not a real market regime.
_SANITY_BOUND = 1e6


def _finite(series) -> bool:
    return bool(np.isfinite(series.dropna()).all())


def steer_features_schema(drivers: Sequence[str] = DRIVER_NAMES) -> pa.DataFrameSchema:
    """Pandera schema for steer_features, for a specific variant's driver set (5 for G10/EM, 7 for CHN).

    A module-level constant can't do this -- CHN's steer_features has 2
    columns (offshore_spread, flows) a schema built from the fixed 5
    DRIVER_NAMES would never validate (or would silently ignore, since
    strict=False). Build one from StrategyConfig.drivers per variant
    instead of importing a single shared schema.
    """
    return pa.DataFrameSchema(
        columns={
            RATE_COLUMN: pa.Column(
                float, nullable=False, checks=[pa.Check.in_range(_RATE_MIN, _RATE_MAX)]
            ),
            **{
                driver: pa.Column(
                    float,
                    nullable=True,
                    checks=[pa.Check(_finite, error=f"{driver} contains a non-finite value")],
                )
                for driver in drivers
            },
            REALIZED_VOLATILITY_COLUMN: pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(1.0)]
            ),
            IS_LOGGED_COLUMN: pa.Column(bool, nullable=False),
        },
        index=pa.Index(pa.DateTime, unique=True),
        coerce=True,
        strict=False,
    )


def steer_estimates_schema(drivers: Sequence[str] = DRIVER_NAMES) -> pa.DataFrameSchema:
    """Pandera schema for gold.steer_estimates, for a specific variant's driver set -- see steer_features_schema."""
    return pa.DataFrameSchema(
        columns={
            "date": pa.Column(pa.DateTime, nullable=False),
            # Column is "universe", not "variant" -- the persisted gold.steer_estimates column
            # name; see steer/orm.py's module docstring.
            "universe": pa.Column(str, nullable=False, checks=pa.Check.isin(VARIANTS)),
            "series_code": pa.Column(str, nullable=False),
            "is_logged": pa.Column(bool, nullable=False),
            "const_coef": pa.Column(
                float, nullable=True, checks=pa.Check.in_range(-_SANITY_BOUND, _SANITY_BOUND)
            ),
            **{
                f"{driver}_coef": pa.Column(
                    float, nullable=True, checks=pa.Check.in_range(-_SANITY_BOUND, _SANITY_BOUND)
                )
                for driver in drivers
            },
            "fitted_value": pa.Column(float, nullable=False),
            "actual_value": pa.Column(float, nullable=False),
            "z_score": pa.Column(float, nullable=False, checks=pa.Check.in_range(-100, 100)),
            "r_squared": pa.Column(float, nullable=False, checks=pa.Check.in_range(0.0, 1.0001)),
            "n_obs": pa.Column(int, nullable=False, checks=pa.Check.gt(0)),
            "cointegration_passed": pa.Column(bool, nullable=False),
            "sign_dropped": pa.Column(bool, nullable=False),
            "dropped_variables": pa.Column(str, nullable=True),
        },
        coerce=True,
        strict=False,
    )


STEER_SIGNALS_SCHEMA = pa.DataFrameSchema(
    columns={
        "date": pa.Column(pa.DateTime, nullable=False),
        # Column is "universe", not "variant" -- the persisted gold.steer_signals column name;
        # see steer/orm.py's module docstring.
        "universe": pa.Column(str, nullable=False, checks=pa.Check.isin(VARIANTS)),
        "series_code": pa.Column(str, nullable=False),
        "signal": pa.Column(
            str, nullable=False, checks=pa.Check.isin((SIGNAL_BUY, SIGNAL_SELL, SIGNAL_NONE))
        ),
        "entry_z_score": pa.Column(float, nullable=False, checks=pa.Check.in_range(-100, 100)),
        "target": pa.Column(float, nullable=True, checks=pa.Check.in_range(_RATE_MIN, _RATE_MAX)),
        "stop_loss": pa.Column(
            float, nullable=True, checks=pa.Check.in_range(_RATE_MIN, _RATE_MAX)
        ),
        "reason": pa.Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
)
