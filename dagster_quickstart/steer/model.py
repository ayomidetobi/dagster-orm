"""Steer/SteerPanel: a model-object facade over the existing STEER pipeline.

Pure Python -- no Dagster -- callable from a script or notebook the same
way as from an asset. `Steer.fit()` is a thin orchestration layer over
functions that already exist and are already exercised by the asset graph
(assets/steer/*.py, assets/availability_asset.py):
dagster_quickstart.availability.storage.read_latest_report,
steer.source.features.fetch_raw_driver_frame/build_steer_features,
steer.analytics.estimation.sign_check_and_reestimate/cointegration_test/generate_signal,
steer.analytics.results.build_steer_result. It does
not reimplement any of them -- same numbers as the asset pipeline, for the
same inputs, by construction (see tests/test_steer_model.py's direct
comparison against a materialized asset graph). fit() reads the availability
report assets/availability_asset.py already wrote (read_latest_report()) rather than
rediscovering pairs and re-resolving every driver role itself -- the script/notebook path and
the Dagster asset graph share one source of truth, not two implementations that could disagree.

SteerResult (steer/analytics/results.py) already is the right object for one pair at
one as_of and is not modified here. SteerPanel is the plural container
its own module docstring describes building manually
(`pd.DataFrame([r.cross_section() for r in results])`) -- this module
formalizes that into `get_cross_section()`, plus a few other views
(`panel`, `signals`, the plotting helpers) over the same underlying
SteerResult objects.

Look-ahead safety: fit(lookback_days=N) re-fits EVERY one of the N dates
independently via window_slice (`timestamp <= as_of`, steer/analytics/estimation.py),
never derives a "historical" z-score by re-scaling a single fit's
residuals. See test_lookback_dates_are_look_ahead_safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

import pandas as pd

from dagster_quickstart.steer.analytics.estimation import CointegrationResult, SteerSignal
from dagster_quickstart.steer.analytics.results import SteerResult
from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC, StrategyConfig
from dagster_quickstart.steer.constants import (
    COINTEGRATION_MODE_EACH,
    COINTEGRATION_MODE_LATEST,
    VARIANT_CHN,
)
from dagster_quickstart.steer.errors import InsufficientDataError

CointegrationMode = Literal["latest", "each"]


#: A cointegration verdict synthesized for a date where the ADF test itself couldn't run
#: (too little trailing history) -- passed=False, matching how
#: assets/steer/cointegration_asset.py's own except-branch (and
#: assets/steer/estimate_asset.py's cointegration_by_pair.get(code, False) default) treat
#: that same case: a pair with a data gap never silently gets an "unknown" (truthy) verdict.
def _insufficient_data_cointegration(as_of: pd.Timestamp) -> CointegrationResult:
    return CointegrationResult(
        as_of=pd.Timestamp(as_of),
        passed=False,
        p_value=1.0,
        test_statistic=0.0,
        critical_values=(0.0, 0.0, 0.0),
        n_obs=0,
    )


@dataclass(frozen=True)
class SteerPanel:
    """Every SteerResult fitted by one Steer.fit() call -- {as_of: {series_code: SteerResult}}.

    signals_by_date holds the SteerSignal (BUY/SELL/NONE + reason) generated
    alongside each SteerResult -- SteerResult itself only keeps the
    resulting target/stop-loss *levels* (upper/lower), not the signal
    enum/reason text, so signals() needs this parallel dict. It's only
    populated by fit() -- SteerPanel.load() (from storage) can rebuild
    every SteerResult but not the signal text, since that isn't part of
    what SteerResult.save() persists; signals() raises a clear error on a
    loaded-not-fitted SteerPanel rather than silently returning nothing.

    blocked is series_code -> reason, for pairs the stored availability report reported
    blocked at fit() time (never fetched, let alone fitted).
    """

    variant: str
    z_threshold: float
    results: Dict[pd.Timestamp, Dict[str, SteerResult]] = field(default_factory=dict)
    signals_by_date: Dict[pd.Timestamp, Dict[str, SteerSignal]] = field(default_factory=dict)
    blocked: Dict[str, str] = field(default_factory=dict)

    @property
    def as_of_dates(self) -> List[pd.Timestamp]:
        """Every fitted date, oldest first."""
        return sorted(self.results.keys())

    def _resolve_as_of(self, as_of: Union[int, str, pd.Timestamp, None]) -> pd.Timestamp:
        dates = self.as_of_dates
        if not dates:
            raise LookupError(f"No fitted dates in this SteerPanel ({self.variant}).")
        if as_of is None:
            return dates[-1]
        if isinstance(as_of, int):
            try:
                return dates[as_of]
            except IndexError:
                raise IndexError(
                    f"as_of index {as_of} out of range for {len(dates)} fitted date(s)."
                ) from None
        resolved = pd.Timestamp(as_of)
        if resolved not in self.results:
            raise KeyError(f"{resolved} is not a fitted date. Fitted dates: {dates}")
        return resolved

    def get_cross_section(self, index: Union[int, str, pd.Timestamp] = -1) -> pd.DataFrame:
        """One row per pair fitted at `index` (a negative/positive position, or a date).

        Built entirely from each SteerResult.cross_section() -- no
        duplicated cross-section logic here.
        """
        as_of = self._resolve_as_of(index)
        by_pair = self.results[as_of]
        return pd.DataFrame([result.cross_section() for result in by_pair.values()])

    def __getitem__(self, series_code: str) -> SteerResult:
        """The SteerResult for `series_code` at the most recent fitted date."""
        return self.get(series_code)

    def get(
        self, series_code: str, as_of: Union[int, str, pd.Timestamp, None] = None
    ) -> SteerResult:
        resolved = self._resolve_as_of(as_of)
        by_pair = self.results[resolved]
        if series_code not in by_pair:
            raise KeyError(f"{series_code!r} was not fitted as of {resolved} ({self.variant}).")
        return by_pair[series_code]

    def signals(self, as_of: Union[int, str, pd.Timestamp, None] = None) -> pd.DataFrame:
        """DataFrame matching STEER_SIGNALS_SCHEMA's columns, at one fitted date (default latest)."""
        resolved = self._resolve_as_of(as_of)
        if resolved not in self.signals_by_date:
            raise LookupError(
                "No signal data for this SteerPanel -- SteerPanel.load() reconstructs "
                "SteerResult objects but not the signal enum/reason text (not part of what "
                "SteerResult persists); signals() only works on a freshly fit() SteerPanel."
            )
        rows = [
            {
                "series_code": series_code,
                "signal": signal.signal,
                "entry_z_score": signal.entry_z_score,
                "target": signal.target,
                "stop_loss": signal.stop_loss,
                "reason": signal.reason,
            }
            for series_code, signal in self.signals_by_date[resolved].items()
        ]
        return pd.DataFrame(rows)

    def panel(self, field_name: str) -> pd.DataFrame:
        """DataFrame indexed by fitted date, one column per pair, for a scalar field.

        `field_name` can be a plain SteerResult attribute (z_score), a
        time-series property whose latest value is what's meant (
        fair_value, fair_value_upper/_lower -- the value as of that row's
        own date), or anything cross_section() exposes (coefficient_<driver>,
        standard_error_<driver>, ...).
        """
        rows: Dict[pd.Timestamp, Dict[str, Any]] = {}
        for as_of, by_pair in self.results.items():
            row: Dict[str, Any] = {}
            for series_code, result in by_pair.items():
                row[series_code] = _scalar_field(result, field_name)
            rows[as_of] = row
        return pd.DataFrame.from_dict(rows, orient="index").sort_index()

    def plot_pair(self, series_code: str, as_of: Union[int, str, pd.Timestamp, None] = None):
        """Delegates to SteerResult.plot() for one pair/date. Returns the Axes."""
        return self.get(series_code, as_of=as_of).plot()

    def plot_z_scores(self, as_of: Union[int, str, pd.Timestamp, None] = None):
        """Horizontal bar of z-score by pair at one date, sorted, with +/-z_threshold lines.

        matplotlib is imported lazily, same as SteerResult.plot(), so it
        stays an optional dependency for callers that never plot.
        """
        import matplotlib.pyplot as plt

        resolved = self._resolve_as_of(as_of)
        by_pair = self.results[resolved]
        z_scores = pd.Series(
            {code: result.z_score for code, result in by_pair.items()}
        ).sort_values()

        colors = [
            "tab:red"
            if value >= self.z_threshold
            else "tab:green"
            if value <= -self.z_threshold
            else "tab:gray"
            for value in z_scores
        ]

        _, ax = plt.subplots(figsize=(8, max(3, 0.35 * len(z_scores))))
        ax.barh(z_scores.index, z_scores.to_numpy(), color=colors)
        ax.axvline(self.z_threshold, color="black", linestyle="--", linewidth=1)
        ax.axvline(-self.z_threshold, color="black", linestyle="--", linewidth=1)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlabel("z-score")
        ax.set_title(f"{self.variant} z-scores as of {resolved.date()}")
        return ax

    def plot_z_history(self, series_codes: Optional[Sequence[str]] = None):
        """z-score over every fitted date, one line per pair, with +/-z_threshold lines.

        Needs more than one fitted date (see Steer.fit()'s lookback_days) --
        a single date is a cross-section, not a history.
        """
        import matplotlib.pyplot as plt

        dates = self.as_of_dates
        if len(dates) <= 1:
            raise ValueError(
                "plot_z_history needs more than one fitted date, but this SteerPanel has "
                f"{len(dates)} -- pass a larger lookback_days to Steer.fit() (it was effectively "
                "1)."
            )

        codes = list(series_codes) if series_codes is not None else sorted(self.results[dates[-1]])

        _, ax = plt.subplots(figsize=(10, 5))
        for code in codes:
            z_by_date = {
                as_of: self.results[as_of][code].z_score
                for as_of in dates
                if code in self.results[as_of]
            }
            if not z_by_date:
                continue
            series = pd.Series(z_by_date).sort_index()
            ax.plot(series.index, series.to_numpy(), marker="o", label=code)

        ax.axhline(self.z_threshold, color="black", linestyle="--", linewidth=1)
        ax.axhline(-self.z_threshold, color="black", linestyle="--", linewidth=1)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_ylabel("z-score")
        ax.set_title(f"{self.variant} z-score history")
        ax.legend()
        return ax

    def save(self, data_api: Any) -> None:
        """Delegates to SteerResult.save() for every fitted (pair, as_of)."""
        for by_pair in self.results.values():
            for result in by_pair.values():
                result.save(data_api)

    @classmethod
    def load(
        cls,
        data_api: Any,
        variant: str,
        as_of: Optional[pd.Timestamp] = None,
    ) -> "SteerPanel":
        """Rebuild a SteerPanel from storage -- one SteerResult.load() per series_code found.

        signals_by_date/blocked come back empty (see SteerPanel's own
        docstring for signals_by_date) -- only what SteerResult.save()
        actually persists round-trips.
        """
        from dagster_quickstart.steer.orm import GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE

        # The persisted gold-table column is still named "universe" (see steer/orm.py's module
        # docstring) -- `variant` is the Python-level name.
        summary = data_api.read_table(
            GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, universe=variant
        ).frame
        if summary.empty:
            return cls(variant=variant, z_threshold=float("nan"))

        summary = summary.copy()
        summary["as_of"] = pd.to_datetime(summary["as_of"])
        if as_of is not None:
            summary = summary[summary["as_of"] == pd.Timestamp(as_of)]

        results: Dict[pd.Timestamp, Dict[str, SteerResult]] = {}
        for series_code in summary["series_code"].unique():
            result = SteerResult.load(data_api, str(series_code), as_of=as_of)
            results.setdefault(result.as_of, {})[str(series_code)] = result

        return cls(variant=variant, z_threshold=float("nan"), results=results)


def _scalar_field(result: SteerResult, field_name: str) -> Any:
    if hasattr(result, field_name):
        value = getattr(result, field_name)
        return float(value.iloc[-1]) if isinstance(value, pd.Series) else value
    cross_section = result.cross_section()
    if field_name in cross_section.index:
        return cross_section[field_name]
    raise AttributeError(
        f"SteerResult has no field {field_name!r} (checked attributes and cross_section())."
    )


class Steer:
    """Fit STEER across every pair in one variant.

    A facade over the existing pipeline -- discovery, features, estimation,
    cointegration, signals -- not a reimplementation of any of it. See
    fit()'s docstring for the per-date/pair sequence, which mirrors
    assets/steer/{silver,gold_features,cointegration,estimate,signal}_asset.py
    exactly (same functions, same arguments, same StrategyConfig fields).
    """

    def __init__(self, data_api: Any, *, variant: str, strategy_config: StrategyConfig) -> None:
        self._data_api = data_api
        self.variant = variant
        self.strategy_config = strategy_config

    @classmethod
    def from_data_api(
        cls, data_api: Any, *, variant: str, strategy_config: StrategyConfig
    ) -> "Steer":
        return cls(data_api, variant=variant, strategy_config=strategy_config)

    def fit(
        self,
        *,
        as_of: Optional[pd.Timestamp] = None,
        lookback_days: int = 1,
        cointegration: CointegrationMode = COINTEGRATION_MODE_LATEST,
        pairs: Optional[Sequence[str]] = None,
    ) -> SteerPanel:
        """Fit every (non-blocked) pair in this variant over the trailing `lookback_days`.

        as_of=None means the latest date available in the fetched data (not
        wall-clock "now") -- the max rate-series date across every pair
        that has any data at all.

        lookback_days=N fits N business days ending at as_of, EACH on its
        own rolling window via window_slice (`timestamp
        <= as_of`, steer/analytics/estimation.py) -- never a single fit's residuals rescaled
        across dates, which would be look-ahead (day t-5's z would depend on
        coefficients estimated using data through day t).

        pairs=None fits every pair the stored availability report doesn't report blocked;
        pass a subset of series_codes to fit fewer. A blocked pair is
        skipped entirely (never fetched) -- see SteerPanel.blocked for
        why, in the same wording steer.source.features.build_silver_frame uses.

        cointegration:
          - "latest" (default): the Engle-Granger test (cointegration_test,
            steer/analytics/estimation.py, itself an ADF call -- regression="c",
            autolag="BIC") runs ONCE per pair, at the final as_of in the
            lookback, and that verdict is reused for every earlier date's
            signal. Cheap: ADF dominates cost (~30x one OLS fit on a
            250-observation window), so testing only once instead of once
            per date is what makes a multi-day lookback affordable for
            interactive use.
          - "each": the test reruns at every date. Required for anything
            backtest-like -- the cointegration gate is part of the signal
            rule itself (generate_signal requires BOTH
            |z| >= z_threshold AND cointegration passed), so freezing the
            verdict at "latest" changes which historical days would have
            traded, silently. Costs one ADF call per pair per date instead
            of one per pair total.
        """
        from dagster_quickstart.availability.report import pairs_from_availability_report
        from dagster_quickstart.availability.storage import read_latest_report
        from dagster_quickstart.steer.analytics.estimation import (
            cointegration_test,
            generate_signal,
            sign_check_and_reestimate,
        )
        from dagster_quickstart.steer.analytics.results import build_steer_result
        from dagster_quickstart.steer.constants import IS_LOGGED_COLUMN, RATE_COLUMN
        from dagster_quickstart.steer.source.features import (
            DriverValues,
            build_steer_features,
            conform_to_business_days,
            fetch_raw_driver_frame,
            required_series_codes,
            resolve_flows_cutover,
        )

        if cointegration not in (COINTEGRATION_MODE_LATEST, COINTEGRATION_MODE_EACH):
            raise ValueError(f'cointegration must be "latest" or "each", got {cointegration!r}')

        config = self.strategy_config

        # Shares one source of truth with the asset graph (assets/availability_asset.py writes
        # this same stored report) instead of re-discovering pairs and re-resolving every driver
        # role itself -- read_latest_report() raises LookupError if nothing's ever been written
        # for this variant, rather than fit() silently returning an empty SteerPanel; a caller
        # that genuinely has no data run yet should see that, not a quietly-empty result.
        report = read_latest_report(self._data_api, self.variant)
        availabilities = pairs_from_availability_report(report, STEER_AVAILABILITY_SPEC)
        if pairs is not None:
            wanted = set(pairs)
            availabilities = [a for a in availabilities if a.series_code in wanted]

        blocked: Dict[str, str] = {}
        available = []
        for availability in availabilities:
            if availability.blocked:
                blocked[availability.series_code] = "blocked: " + "; ".join(
                    availability.block_reasons
                )
            else:
                available.append(availability)

        if not available:
            return SteerPanel(variant=self.variant, z_threshold=config.z_threshold, blocked=blocked)

        all_series_codes = required_series_codes(((a.series_code, a) for a in available), config)
        driver_values = DriverValues.load(self._data_api, all_series_codes)

        chn_flows_cutover = None
        if self.variant == VARIANT_CHN:
            try:
                chn_flows_cutover = resolve_flows_cutover(self._data_api)
            except ValueError:
                chn_flows_cutover = None

        pair_features: Dict[str, pd.DataFrame] = {}
        for availability in available:
            raw = fetch_raw_driver_frame(
                driver_values,
                availability.series_code,
                config,
                availability,
                chn_flows_cutover=chn_flows_cutover,
            )
            if raw.empty:
                continue
            conformed = conform_to_business_days(raw, primary_column=RATE_COLUMN)
            if conformed.empty:
                continue
            features = build_steer_features(
                conformed,
                drivers=config.drivers,
                logged_rate_threshold=config.logged_rate_threshold,
                vol_window_days=config.logged_rate_vol_window_days,
            )
            pair_features[availability.series_code] = features

        if not pair_features:
            return SteerPanel(variant=self.variant, z_threshold=config.z_threshold, blocked=blocked)

        resolved_as_of = (
            pd.Timestamp(as_of)
            if as_of is not None
            else max(features.index.max() for features in pair_features.values())
        )
        calendar = sorted(
            {timestamp for features in pair_features.values() for timestamp in features.index}
        )
        calendar = [timestamp for timestamp in calendar if timestamp <= resolved_as_of]
        if not calendar:
            raise InsufficientDataError(
                f"No data on or before {resolved_as_of} for variant {self.variant!r}."
            )
        fit_dates = calendar[-lookback_days:]

        driver_columns = list(config.drivers)

        latest_cointegration: Dict[str, CointegrationResult] = {}
        if cointegration == COINTEGRATION_MODE_LATEST:
            latest_date = fit_dates[-1]
            for series_code, features in pair_features.items():
                if latest_date not in features.index:
                    continue
                is_logged = bool(features[IS_LOGGED_COLUMN].loc[:latest_date].iloc[-1])
                latest_cointegration[series_code] = self._safe_cointegration_test(
                    cointegration_test,
                    features[RATE_COLUMN],
                    features[driver_columns],
                    as_of=latest_date,
                    is_logged=is_logged,
                    config=config,
                )

        results: Dict[pd.Timestamp, Dict[str, SteerResult]] = {}
        signals_by_date: Dict[pd.Timestamp, Dict[str, SteerSignal]] = {}

        for fit_date in fit_dates:
            date_results: Dict[str, SteerResult] = {}
            date_signals: Dict[str, SteerSignal] = {}

            for series_code, features in pair_features.items():
                trailing = features.loc[:fit_date]
                if trailing.empty:
                    continue

                rate = features[RATE_COLUMN]
                drivers = features[driver_columns]
                is_logged = bool(trailing[IS_LOGGED_COLUMN].iloc[-1])

                try:
                    estimate = sign_check_and_reestimate(
                        rate,
                        drivers,
                        as_of=fit_date,
                        window_months=config.window_months,
                        is_logged=is_logged,
                        expected_signs=config.expected_signs,
                        min_observations=config.min_observations,
                    )
                except InsufficientDataError:
                    continue

                coint_result: CointegrationResult
                if cointegration == COINTEGRATION_MODE_EACH:
                    coint_result = self._safe_cointegration_test(
                        cointegration_test,
                        rate,
                        drivers,
                        as_of=fit_date,
                        is_logged=is_logged,
                        config=config,
                    )
                else:
                    coint_result = latest_cointegration.get(
                        series_code
                    ) or _insufficient_data_cointegration(fit_date)

                current_rate = float(rate.loc[:fit_date].iloc[-1])
                signal = generate_signal(
                    estimate,
                    coint_result,
                    current_rate=current_rate,
                    z_threshold=config.z_threshold,
                    stop_reward_ratio=config.stop_reward_ratio,
                )

                result = build_steer_result(
                    series_code,
                    self.variant,
                    rate,
                    drivers,
                    estimate=estimate,
                    window_months=config.window_months,
                    z_threshold=config.z_threshold,
                    cointegration=coint_result,
                    signal_target=signal.target,
                    signal_stop_loss=signal.stop_loss,
                )
                date_results[series_code] = result
                date_signals[series_code] = signal

            if date_results:
                results[fit_date] = date_results
                signals_by_date[fit_date] = date_signals

        return SteerPanel(
            variant=self.variant,
            z_threshold=config.z_threshold,
            results=results,
            signals_by_date=signals_by_date,
            blocked=blocked,
        )

    @staticmethod
    def _safe_cointegration_test(
        cointegration_test, rate, drivers, *, as_of, is_logged, config
    ) -> CointegrationResult:
        """cointegration_test(), or a synthetic passed=False verdict on InsufficientDataError --
        see _insufficient_data_cointegration's docstring for why that (not a missing/None
        result) matches the asset pipeline's own handling of the same case."""
        try:
            return cointegration_test(
                rate,
                drivers,
                as_of=as_of,
                window_months=config.window_months,
                is_logged=is_logged,
                significance=config.cointegration_significance,
                min_observations=config.min_observations,
            )
        except InsufficientDataError:
            return _insufficient_data_cointegration(as_of)
