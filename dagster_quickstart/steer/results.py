"""SteerResult: one consolidated, retrievable artifact per currency pair.

Bundles a pair's spot rate, this universe's driver series (whatever
StrategyConfig.drivers holds for it -- 5 for G10/EM, 7 for CHN), the
regression's design/response/fitted/residual, and its statistical
diagnostics (coefficients, standard errors, p-values, z-score,
upper_bound/lower_bound) into one object per pair -- everything already
computed elsewhere in steer/ (features.py, estimation.py, signals.py) via
build_steer_result(), just packaged for easy per-pair retrieval
(SteerResult.save()/.load()), cross-pair comparison (cross_section()), and
plotting (plot()).

upper_bound/lower_bound = fitted +/- z_threshold * residual_std -- the
SAME boundary steer.signals.generate_signal uses to decide whether a
BUY/SELL fires (|z_score| >= z_threshold), matching the reference
production model exactly (Figs 16/17 of the published methodology shade
this exact band). This used to be
statsmodels.tsa.stattools.summary_frame()'s confidence interval on the
fitted MEAN, which measures parameter uncertainty, not residual
dispersion -- on a ~250-observation window that interval is roughly 5x
narrower than the real trigger band, so the shaded region on
SteerResult.plot() had no relationship to where signals actually fire.

Drivers are held as a `drivers: dict[str, pd.Series]` (driver name ->
series), not fixed dataclass fields -- a fixed 5-field shape (the old
design) has no room for CHN's 2 extra drivers (offshore_spread, flows) and
would silently need per-universe subclasses or None-padding. `design`
derives the regression's X matrix (drivers + a constant) from this dict on
demand.

Every time-series field (spot, every entry of `drivers`, response, fitted,
residual, upper_bound, lower_bound) shares the identical DatetimeIndex --
the trailing regression window ending at `as_of` (see
steer.estimation.window_slice) -- so they concatenate/plot directly with
no reindexing.

build_steer_result() takes an already-computed SteerEstimate (from
steer.estimation.sign_check_and_reestimate) rather than independently
re-fitting and re-deriving z_score/dropped_variables itself -- the previous
version re-fit with *every* driver and no sign check, so its z_score could
silently disagree with the "real" SteerEstimate.z_score whenever a driver
had been dropped there, and it carried no dropped_variables/cointegration
information at all. Taking the estimate as input makes z_score/
dropped_variables agree by construction; this module still fits its own
OLS (over the identical window, on the identical kept-driver columns) to
get the *full* windowed design/response/fitted/residual series plus
per-coefficient p-values/standard errors and the fitted +/- z_threshold *
residual_std trigger band that SteerEstimate doesn't expose (only its
latest-day z_score) -- residual_std uses ddof=0, matching estimate_steer's
z_score calculation exactly (see steer.estimation's module docstring).

markov_state is a reserved field, always None today -- no Markov
regime-switching model exists anywhere in this codebase; it's a
placeholder for a future addition, not a fabricated value.

Persisted via DataAPI.write_table()/.read_table() (rewrite/data_api) into 2 DuckLake gold
tables -- gold.steer_results (long-form, one row per series_code/as_of/date, the time-series
fields) and gold.steer_result_summary (one row per series_code/as_of, the scalar/cross-sectional
fields) -- the same mechanism gold.steer_estimates/gold.steer_signals already use, and the same
DuckLake connection the rest of a run's DataAPI calls share (no second attach). DuckLake itself
is Parquet-backed on disk (S3), so this is real Parquet storage under the hood, not a bespoke
file format. Both tables are append-only, like the rest of this catalog: a re-run for the same
pair/as_of writes a new snapshot rather than overwriting -- load() returns the most recent one by
default. Different universes writing different driver sets to the same table is exactly what
write_table()'s column-widening handles -- see
rewrite.data_api.repositories.generic_table_repository.GenericTableRepository.write()'s docstring.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm

from dagster_quickstart.steer.estimation import CointegrationResult, SteerEstimate, window_slice
from dagster_quickstart.steer.storage import GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, STEER_RESULTS_TABLE

#: to_frame()'s fixed (non-driver) time-series columns -- everything else
#: in a loaded gold.steer_results row is a driver series (see load()).
#: fair_value is included here (not a dataclass field -- it's derived from
#: fitted/is_logged via the fair_value property) purely so load()'s driver
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


@dataclass(frozen=True)
class SteerResult:
    """One pair's full STEER snapshot -- prices, drivers, regression diagnostics.

    fx is an alias for spot (same series, both names are common FX
    terminology). design is derived on demand from `drivers` (+ a constant
    column) rather than stored twice.

    upper/lower and upper_bound/lower_bound are now the same KIND of
    object -- fitted +/- z_threshold * residual_std, the reference
    production model's actual trading-trigger boundary (see module
    docstring) -- they differ only in shape: upper_bound/lower_bound are
    the full windowed *series* (the band drawn on a chart), while
    upper/lower are a single scalar *level* (steer.signals.generate_signal's
    Signal.target/.stop_loss for the specific signal actually generated, if
    passed to build_steer_result). upper/lower are optional: a pair that
    hasn't had a signal generated yet (e.g. cointegration failed) still
    gets a full SteerResult, just with upper/lower left None.
    """

    series_code: str
    universe: str
    as_of: pd.Timestamp
    is_logged: bool

    spot: pd.Series
    #: driver name -> series (this universe's StrategyConfig.drivers set).
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
    #: build_steer_result's docstring for why this now always agrees with
    #: the SteerEstimate it was built from.
    dropped_variables: Tuple[str, ...] = ()
    cointegration_passed: Optional[bool] = None
    upper: Optional[float] = None
    lower: Optional[float] = None
    markov_state: Optional[str] = None

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
        dropped_variables, cointegration_passed, and every coefficient/standard_error/p_value with a
        `{name}_<driver>` suffix). Concat several pairs' cross_section() rows (e.g.
        `pd.DataFrame([r.cross_section() for r in results])`) to compare pairs side by side at one
        point in time -- the econometric sense of "cross section", as opposed to one pair's own time
        series. fair_value is the latest rate-level fitted value -- the number anyone comparing pairs
        side by side actually wants (see the fair_value property).
        """
        row: dict = {
            "series_code": self.series_code,
            "universe": self.universe,
            "as_of": self.as_of,
            "is_logged": self.is_logged,
            "z_score": self.z_score,
            "fair_value": float(self.fair_value.iloc[-1]),
            "dropped_variables": ",".join(self.dropped_variables),
            "cointegration_passed": self.cointegration_passed,
            "upper": self.upper,
            "lower": self.lower,
            "markov_state": self.markov_state,
        }
        for name, value in self.coefficient.items():
            row[f"coefficient_{name}"] = value
        for name, value in self.standard_error.items():
            row[f"standard_error_{name}"] = value
        for name, value in self.p_values.items():
            row[f"p_value_{name}"] = value
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
        the current rate and the stop ABOVE it (steer.signals.generate_signal);
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
        ax.set_title(f"{self.series_code} ({self.universe}) -- z={self.z_score:.2f}")
        ax.legend()
        return ax

    def save(self, data_api: Any) -> None:
        """Write this result via data_api.write_table() -- see module docstring.

        Writes to gold.steer_results (the time-series fields, via
        to_frame()) and gold.steer_result_summary (the scalar fields, via
        cross_section()) -- both keyed by series_code/universe/as_of.
        """
        timeseries = self.to_frame().copy()
        timeseries.index.name = "date"
        timeseries = timeseries.reset_index()
        timeseries.insert(0, "as_of", self.as_of)
        timeseries.insert(0, "universe", self.universe)
        timeseries.insert(0, "series_code", self.series_code)
        data_api.write_table(GOLD_SCHEMA, STEER_RESULTS_TABLE, timeseries)

        summary = pd.DataFrame([self.cross_section()])
        data_api.write_table(GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, summary)

    @classmethod
    def load(
        cls, data_api: Any, series_code: str, *, as_of: Optional[pd.Timestamp] = None
    ) -> "SteerResult":
        """Load one pair's SteerResult back via data_api.read_table() -- see save()/module docstring.

        Both tables are append-only snapshots -- without `as_of`, the most
        recent snapshot for this series_code is returned; pass `as_of` to
        load a specific historical one. Raises LookupError if no snapshot
        matches. Driver columns are inferred as "everything in
        gold.steer_results that isn't spot/response/fitted/residual/
        upper_bound/lower_bound/date/as_of/universe/series_code" -- there's
        no fixed driver-name list to check against any more (see the
        module docstring).
        """
        summary = data_api.read_table(
            GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, series_code=series_code
        ).frame
        if summary.empty:
            raise LookupError(f"No SteerResult found in DuckLake for {series_code!r}.")
        summary["as_of"] = pd.to_datetime(summary["as_of"])
        if as_of is not None:
            summary = summary[summary["as_of"] == pd.Timestamp(as_of)]
            if summary.empty:
                raise LookupError(f"No SteerResult found for {series_code!r} as of {as_of}.")
        row = summary.sort_values("as_of").iloc[-1]
        row_as_of = row["as_of"]

        timeseries = data_api.read_table(
            GOLD_SCHEMA, STEER_RESULTS_TABLE, series_code=series_code
        ).frame
        timeseries["as_of"] = pd.to_datetime(timeseries["as_of"])
        timeseries = timeseries[timeseries["as_of"] == row_as_of].copy()
        timeseries["date"] = pd.to_datetime(timeseries["date"])
        timeseries = timeseries.set_index("date").sort_index()

        driver_names = [
            column
            for column in timeseries.columns
            if column not in _FIXED_TIMESERIES_COLUMNS
            and column not in ("as_of", "universe", "series_code")
            # A column entirely NaN for this pair is padding from another
            # universe's wider driver set sharing this physical table (see
            # GenericTableRepository.write()'s column widening), not one of
            # this pair's own drivers.
            and not timeseries[column].isna().all()
        ]
        drivers = {name: timeseries[name] for name in driver_names}

        def _prefixed(prefix: str) -> pd.Series:
            # pd.notna(v) drops padding from another universe's wider
            # driver set sharing this physical table (see the driver_names
            # comment above) -- not a real coefficient/std-error/p-value
            # for this pair.
            return pd.Series(
                {
                    str(k)[len(prefix) :]: v
                    for k, v in row.items()
                    if str(k).startswith(prefix) and pd.notna(v)
                }
            )

        def _optional_float(value: object) -> Optional[float]:
            return float(value) if pd.notna(value) else None  # type: ignore[call-overload,arg-type]

        dropped = row.get("dropped_variables")
        dropped_variables = tuple(dropped.split(",")) if isinstance(dropped, str) and dropped else ()

        return cls(
            series_code=series_code,
            universe=row["universe"],
            as_of=row_as_of,
            is_logged=bool(row["is_logged"]),
            spot=timeseries["spot"],
            drivers=drivers,
            response=timeseries["response"],
            fitted=timeseries["fitted"],
            residual=timeseries["residual"],
            upper_bound=timeseries["upper_bound"],
            lower_bound=timeseries["lower_bound"],
            coefficient=_prefixed("coefficient_"),
            standard_error=_prefixed("standard_error_"),
            p_values=_prefixed("p_value_"),
            z_score=float(row["z_score"]),
            dropped_variables=dropped_variables,
            cointegration_passed=(
                bool(row["cointegration_passed"]) if pd.notna(row.get("cointegration_passed")) else None
            ),
            upper=_optional_float(row.get("upper")),
            lower=_optional_float(row.get("lower")),
            markov_state=row["markov_state"] if pd.notna(row.get("markov_state")) else None,
        )


def build_steer_result(
    series_code: str,
    universe: str,
    rate: pd.Series,
    drivers: pd.DataFrame,
    *,
    estimate: SteerEstimate,
    window_months: int,
    z_threshold: float = 1.5,
    cointegration: Optional[CointegrationResult] = None,
    signal_target: Optional[float] = None,
    signal_stop_loss: Optional[float] = None,
) -> SteerResult:
    """Build one pair's SteerResult from its raw rate + driver frame and an already-computed SteerEstimate.

    `drivers` has this pair's universe's driver columns (StrategyConfig.drivers
    -- 5 for G10/EM, 7 for CHN); `estimate` is
    steer.estimation.sign_check_and_reestimate's result for the identical
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
    steer.signals.generate_signal actually used to decide BUY/SELL/NONE.

    cointegration (steer.estimation.cointegration_test's result for this
    pair/as_of, if available) becomes cointegration_passed.
    signal_target/signal_stop_loss (from steer.signals.generate_signal's
    Signal.target/.stop_loss, if a signal was already generated for this
    pair) become upper/lower; pass neither to leave them None.
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
    # exactly (see steer.estimation's module docstring), so this band and
    # SteerEstimate.z_score agree on what "one residual_std" means.
    residual_std = float(residual.std(ddof=0))

    driver_series = {name: drivers_f[name].reindex(windowed.index) for name in drivers_f.columns}

    return SteerResult(
        series_code=series_code,
        universe=universe,
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
        upper=signal_target,
        lower=signal_stop_loss,
        markov_state=None,
    )
