"""Gold-layer STEER feature construction: raw driver series -> a model-ready table.

Turns a wide frame of already-conformed raw series (see
assets/steer/silver_asset.py for the silver-layer alignment step) into the
per-pair STEER feature table: this universe's drivers (see
StrategyConfig.drivers -- 5 for G10/EM, 7 for CHN) plus a rolling
`is_logged` flag, one row per date.

Driver construction is deliberately NOT symmetric between G10 and EM/CHN --
this follows the published STEER methodology, not a simplification:

  - interest_rate_differential: base_swap_2y - quote_swap_2y, every universe.
  - yield_curve_or_cds: G10 is a genuine curve-slope differential,
    (base_3m - base_10y) - (quote_3m - quote_10y); EM/CHN is the non-USD
    leg's 5Y sovereign CDS *level* (not a difference -- EM/CHN pairs are
    always vs. USD, and USD has no CDS quote of its own in this catalog).
    yield_curve_or_cds used to be a literal duplicate of
    interest_rate_differential (same object assigned to both columns) --
    that was perfect collinearity (statsmodels splits the coefficient
    arbitrarily between the two, both standard errors inflate), not a
    documented simplification, and is fixed here.
  - local_equity: G10 is log(base_msci) - log(quote_msci); EM/CHN is
    log(non_usd_msci) alone (single leg, same USD-quote reasoning as CDS).
  - global_equity / commodity: log(single global series), identical
    across every pair/universe (see steer/config.py's GLOBAL_DRIVERS).

Every log/differential input is read out of a pair's PairAvailability
(steer/discovery.py) -- resolved *by role*, never a hardcoded series_code.
A driver missing any input it needs is filled with pd.NA for that pair,
never substituted with a proxy (e.g. the global equity series standing in
for a missing local_equity) -- see fetch_raw_driver_frame.

CHN also gets two extra drivers (offshore_spread, flows) -- see
build_chn_offshore_spread/build_chn_flows below and steer/config.py's CHN
YAML for why they're not in the 5 canonical DRIVER_NAMES.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import structlog

from dagster_quickstart.steer.config import StrategyConfig
from dagster_quickstart.steer.constants import (
    CURRENCY_USD,
    DRIVER_COMMODITY,
    DRIVER_FLOWS,
    DRIVER_GLOBAL_EQUITY,
    DRIVER_INTEREST_RATE_DIFFERENTIAL,
    DRIVER_LOCAL_EQUITY,
    DRIVER_OFFSHORE_SPREAD,
    DRIVER_YIELD_CURVE_OR_CDS,
    FLOW_BUY_SELL_SERIES,
    FLOW_TURNOVER_SERIES,
    IS_LOGGED_COLUMN,
    LEG_BASE,
    LEG_QUOTE,
    OFFSHORE_SPREAD_SERIES,
    ONSHORE_SPREAD_SERIES,
    RATE_COLUMN,
    REALIZED_VOLATILITY_COLUMN,
    ROLE_CDS_5Y,
    ROLE_LOCAL_EQUITY,
    ROLE_RATE_3M,
    ROLE_SWAP_2Y,
    ROLE_YIELD_10Y,
    UNIVERSE_CHN,
    UNIVERSE_G10,
)
from dagster_quickstart.steer.discovery import PairAvailability

logger = structlog.get_logger(__name__)


def compute_realized_volatility(rate: pd.Series, *, window_days: int) -> pd.Series:
    """Trailing rolling mean absolute daily % change of `rate`, in decimal terms (0.01 == 1%).

    NaN for the first `window_days` rows of any series (not enough history
    yet for a full window) -- callers should treat NaN as "not logged" (see
    should_use_logged_rate), not as zero volatility.
    """
    return rate.pct_change().abs().rolling(window_days, min_periods=window_days).mean()


def should_use_logged_rate(
    realized_volatility: pd.Series,
    *,
    as_of: pd.Timestamp,
    threshold: float,
) -> bool:
    """True if `realized_volatility` as of `as_of` exceeds `threshold` (log-space regression); False otherwise.

    False (level regression) whenever there isn't a full volatility window
    yet -- a conservative default rather than guessing log vs. level from
    partial data.
    """
    as_of = pd.Timestamp(as_of)
    trailing = realized_volatility.loc[:as_of]
    if trailing.empty or pd.isna(trailing.iloc[-1]):
        return False
    return bool(trailing.iloc[-1] > threshold)


def build_steer_features(
    raw: pd.DataFrame,
    *,
    drivers: Sequence[str],
    logged_rate_threshold: float,
    vol_window_days: int = 20,
) -> pd.DataFrame:
    """Build the gold-layer STEER feature table for one currency pair.

    `raw` must be indexed by date and have one column per `drivers` plus
    RATE_COLUMN -- already resolved from series_codes and aligned/conformed
    (see assets/steer/silver_asset.py). `drivers` is this pair's universe's
    driver set (StrategyConfig.drivers -- 5 for G10/EM, 7 for CHN), not a
    fixed module constant, so CHN's extra offshore_spread/flows columns
    survive instead of being silently dropped. Adds realized_volatility and
    is_logged (recomputed per day, so a pair can cross the log/level
    threshold over time -- estimate_steer/cointegration_test are told which
    regime applies for the specific `as_of` they're evaluating, via that
    day's is_logged value).

    Raises KeyError if `raw` is missing RATE_COLUMN or any of `drivers` --
    validated more thoroughly downstream by steer.schemas.steer_features_schema
    as a Dagster asset check.
    """
    feature_columns = (RATE_COLUMN,) + tuple(drivers)
    missing = [column for column in feature_columns if column not in raw.columns]
    if missing:
        raise KeyError(f"raw is missing required column(s): {missing}")

    features = raw[list(feature_columns)].copy()
    volatility = compute_realized_volatility(features[RATE_COLUMN], window_days=vol_window_days)
    features[REALIZED_VOLATILITY_COLUMN] = volatility
    features[IS_LOGGED_COLUMN] = volatility > logged_rate_threshold
    features[IS_LOGGED_COLUMN] = features[IS_LOGGED_COLUMN].fillna(False)
    return features


def _safe_log(series: pd.Series) -> pd.Series:
    return pd.Series(np.log(series.astype(float)), index=series.index)


def resolve_flows_cutover(data_api: Any) -> pd.Timestamp:
    """The date CHN's `flows` driver switches formula (net flows -> total turnover).

    Read from the `valid_to` shared by FLOW_BUY_SELL_SERIES in metadata --
    never hardcoded (HKEX's actual cutover date lives in the catalog, not
    in this code). Raises ValueError if those 4 series don't share exactly
    one valid_to.
    """
    metadata = data_api.get_metadata(series_code=list(FLOW_BUY_SELL_SERIES)).frame
    valid_to = pd.to_datetime(metadata.get("valid_to"), errors="coerce").dropna().unique()
    if len(valid_to) != 1:
        raise ValueError(
            f"Expected exactly one valid_to shared by {FLOW_BUY_SELL_SERIES}, "
            f"found {list(valid_to)!r} -- can't determine the flows regime cutover."
        )
    return pd.Timestamp(valid_to[0])


def build_chn_flows(
    *,
    shanghai_buy: pd.Series,
    shenzhen_buy: pd.Series,
    shanghai_sell: pd.Series,
    shenzhen_sell: pd.Series,
    shanghai_turnover: pd.Series,
    shenzhen_turnover: pd.Series,
    cutover: pd.Timestamp,
) -> pd.Series:
    """CHN's `flows` driver: net buy-sell flows before `cutover`, total turnover from `cutover` onward.

    Net flows and total turnover are negatively related (opposite regimes,
    not a continuation of the same quantity -- the published note reports
    R^2=0.4484 with a negative slope between them), so a single `flows`
    column spanning both eras carries two different variables under one
    name; a rolling estimation window that straddles `cutover` mixes both
    regimes for the length of that window -- see fetch_raw_driver_frame for
    where that's surfaced as a warning, since this function itself has no
    `as_of`/window to check against.
    """
    cutover = pd.Timestamp(cutover)
    net_flows = (shanghai_buy + shenzhen_buy) - (shanghai_sell + shenzhen_sell)
    turnover = shanghai_turnover + shenzhen_turnover
    index = net_flows.index.union(turnover.index)

    flows = pd.Series(np.nan, index=index, dtype=float)
    pre_cutover = index < cutover
    flows.loc[pre_cutover] = net_flows.reindex(index).loc[pre_cutover]
    flows.loc[~pre_cutover] = turnover.reindex(index).loc[~pre_cutover]
    return flows


def build_chn_offshore_spread(offshore: pd.Series, onshore: pd.Series) -> pd.Series:
    """CHN's `offshore_spread` driver: the CNH-CNY basis (offshore 3M forward points - onshore)."""
    return offshore - onshore


def _non_usd_leg(availability: PairAvailability) -> Optional[str]:
    if availability.base_currency == CURRENCY_USD and availability.quote_currency != CURRENCY_USD:
        return LEG_QUOTE
    if availability.quote_currency == CURRENCY_USD and availability.base_currency != CURRENCY_USD:
        return LEG_BASE
    return None


def _build_series_by_column(
    series_code: str,
    strategy_config: StrategyConfig,
    availability: PairAvailability,
) -> Dict[str, str]:
    """The column-name -> series_code mapping this pair needs (rate + global drivers +
    whatever driver-role series `availability` resolved, plus CHN's fixed offshore_spread/
    flows series). Extracted out of fetch_raw_driver_frame so required_series_codes() can
    collect every pair's needed series upfront without duplicating this logic.
    """
    universe = strategy_config.universe
    series_by_column: Dict[str, str] = {
        RATE_COLUMN: series_code,
        DRIVER_GLOBAL_EQUITY: strategy_config.global_equity_series,
        DRIVER_COMMODITY: strategy_config.commodity_series,
    }

    base_swap = availability.get(LEG_BASE, ROLE_SWAP_2Y)
    quote_swap = availability.get(LEG_QUOTE, ROLE_SWAP_2Y)
    if base_swap and quote_swap:
        series_by_column["_base_swap_2y"] = base_swap
        series_by_column["_quote_swap_2y"] = quote_swap

    if universe == UNIVERSE_G10:
        base_3m = availability.get(LEG_BASE, ROLE_RATE_3M)
        quote_3m = availability.get(LEG_QUOTE, ROLE_RATE_3M)
        base_10y = availability.get(LEG_BASE, ROLE_YIELD_10Y)
        quote_10y = availability.get(LEG_QUOTE, ROLE_YIELD_10Y)
        if base_3m and quote_3m and base_10y and quote_10y:
            series_by_column["_base_3m"] = base_3m
            series_by_column["_quote_3m"] = quote_3m
            series_by_column["_base_10y"] = base_10y
            series_by_column["_quote_10y"] = quote_10y

        base_equity = availability.get(LEG_BASE, ROLE_LOCAL_EQUITY)
        quote_equity = availability.get(LEG_QUOTE, ROLE_LOCAL_EQUITY)
        if base_equity and quote_equity:
            series_by_column["_base_equity"] = base_equity
            series_by_column["_quote_equity"] = quote_equity
    else:
        non_usd_leg = _non_usd_leg(availability)
        cds = availability.get(non_usd_leg, ROLE_CDS_5Y) if non_usd_leg else None
        if cds:
            series_by_column["_non_usd_cds"] = cds
        non_usd_equity = availability.get(non_usd_leg, ROLE_LOCAL_EQUITY) if non_usd_leg else None
        if non_usd_equity:
            series_by_column["_non_usd_equity"] = non_usd_equity

    if universe == UNIVERSE_CHN:
        series_by_column["_offshore_spread"] = OFFSHORE_SPREAD_SERIES
        series_by_column["_onshore_spread"] = ONSHORE_SPREAD_SERIES
        for code in FLOW_BUY_SELL_SERIES + FLOW_TURNOVER_SERIES:
            series_by_column[f"_flow_{code}"] = code

    return series_by_column


def required_series_codes(
    pairs: Iterable[Tuple[str, PairAvailability]],
    strategy_config: StrategyConfig,
) -> set:
    """Every series_code fetch_raw_driver_frame would need across all of `pairs`
    (series_code, availability) -- for DriverValues.load(), so one universe's whole run
    issues a single get_values() call instead of one per pair. USD's role series and the
    global_equity/commodity series naturally collapse to one entry each here even though
    they're referenced by every pair, since this is a set.
    """
    codes: set = set()
    for series_code, availability in pairs:
        codes.update(_build_series_by_column(series_code, strategy_config, availability).values())
    return codes


class DriverValues:
    """One wide value frame for the whole run: index=timestamp, columns=series_code.

    Replaces one get_values() call per pair with a single call for the whole run --
    fetch_raw_driver_frame takes this instead of a data_api, and never queries inside a
    per-pair loop. Per-pair driver frames are column selections from this, not fresh queries.
    """

    def __init__(self, wide: pd.DataFrame) -> None:
        self._wide = wide

    @classmethod
    def load(cls, data_api: Any, series_codes: Iterable[str], *, start: Any = None, end: Any = None) -> "DriverValues":
        """Fetch every series in `series_codes` in one call.

        Deliberately does NOT pass ticker_source -- get_values() resolves the vendor
        internally per series from each row's source_default, so one call returns BBG, HAWK
        and Macrobond series together. Passing an explicit ticker_source would pin every
        series to one vendor and silently drop everything sourced from the others (e.g. the
        12 real 5Y sovereign CDS rows, which are HAWK -- emptying EM driver 2 for every EM
        pair -- and the 4 softs, which are Macrobond).
        """
        wide = data_api.get_values(sorted(set(series_codes)))
        if start is not None:
            wide = wide.loc[wide.index >= pd.Timestamp(start)]
        if end is not None:
            wide = wide.loc[wide.index <= pd.Timestamp(end)]
        return cls(wide)

    def select(self, mapping: Dict[str, str]) -> pd.DataFrame:
        """mapping: canonical column name -> series_code. Missing series become NaN columns.

        Never substitutes another series for a missing one -- a series_code absent from the
        wide frame (unresolved role, or resolved but never actually ingested) produces a NaN
        column, preserving fetch_raw_driver_frame's existing "blocked/partial pair still gets
        a well-formed frame" contract.
        """
        out = pd.DataFrame(index=self._wide.index)
        for column, code in mapping.items():
            out[column] = self._wide[code] if code in self._wide.columns else pd.NA
        return out


def fetch_raw_driver_frame(
    driver_values: DriverValues,
    series_code: str,
    strategy_config: StrategyConfig,
    availability: PairAvailability,
    *,
    start: Any = None,
    end: Any = None,
    chn_flows_cutover: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Build one pair's rate + universe drivers (strategy_config.drivers columns) from an already-loaded DriverValues.

    `driver_values` is a DriverValues already loaded for this whole run (see
    DriverValues.load) -- this function never queries DuckLake itself.
    Callers should already have checked `availability.blocked` (see
    assets/steer/silver_asset.py) -- this builds whatever it can
    regardless, filling anything unavailable with pd.NA, so a caller that
    calls this on a blocked pair anyway still gets a well-formed (if
    unusable) frame rather than a crash. See the module docstring for the
    exact G10 vs. EM/CHN driver formulas.

    `chn_flows_cutover` is required (raises ValueError if omitted) whenever
    this is a CHN pair with the 6 flow series available -- resolve it once
    per run (steer.features.resolve_flows_cutover) and pass it in, rather
    than this function doing its own get_metadata() call per pair.
    """
    universe = strategy_config.universe
    series_by_column = _build_series_by_column(series_code, strategy_config, availability)

    wide = driver_values.select(series_by_column)
    if start is not None:
        wide = wide.loc[wide.index >= pd.Timestamp(start)]
    if end is not None:
        wide = wide.loc[wide.index <= pd.Timestamp(end)]
    if wide.empty:
        return wide

    renamed = wide

    def has_data(column: str) -> bool:
        """True if `column` exists AND has at least one real (non-NaN) value.

        DriverValues.select() always creates a column for every requested
        mapping entry -- unlike the old data_api.get_values() call, which
        simply omitted a column that had zero value rows. A plain "column
        in renamed" check would therefore always be True here regardless
        of whether real data was actually returned; checking for at least
        one non-null value restores the original "does this driver
        actually have data" semantics.
        """
        return column in renamed.columns and bool(renamed[column].notna().any())

    features = pd.DataFrame(index=renamed.index)
    features[RATE_COLUMN] = renamed.get(RATE_COLUMN)
    features[DRIVER_GLOBAL_EQUITY] = (
        _safe_log(renamed[DRIVER_GLOBAL_EQUITY]) if has_data(DRIVER_GLOBAL_EQUITY) else pd.NA
    )
    features[DRIVER_COMMODITY] = (
        _safe_log(renamed[DRIVER_COMMODITY]) if has_data(DRIVER_COMMODITY) else pd.NA
    )

    if has_data("_base_swap_2y") and has_data("_quote_swap_2y"):
        features[DRIVER_INTEREST_RATE_DIFFERENTIAL] = renamed["_base_swap_2y"] - renamed["_quote_swap_2y"]
    else:
        features[DRIVER_INTEREST_RATE_DIFFERENTIAL] = pd.NA

    if universe == UNIVERSE_G10:
        have_curve = all(has_data(c) for c in ("_base_3m", "_quote_3m", "_base_10y", "_quote_10y"))
        if have_curve:
            base_slope = renamed["_base_3m"] - renamed["_base_10y"]
            quote_slope = renamed["_quote_3m"] - renamed["_quote_10y"]
            features[DRIVER_YIELD_CURVE_OR_CDS] = base_slope - quote_slope
        else:
            features[DRIVER_YIELD_CURVE_OR_CDS] = pd.NA

        if has_data("_base_equity") and has_data("_quote_equity"):
            features[DRIVER_LOCAL_EQUITY] = _safe_log(renamed["_base_equity"]) - _safe_log(
                renamed["_quote_equity"]
            )
        else:
            features[DRIVER_LOCAL_EQUITY] = pd.NA
    else:
        features[DRIVER_YIELD_CURVE_OR_CDS] = (
            renamed["_non_usd_cds"] if has_data("_non_usd_cds") else pd.NA
        )
        features[DRIVER_LOCAL_EQUITY] = (
            _safe_log(renamed["_non_usd_equity"]) if has_data("_non_usd_equity") else pd.NA
        )

    if universe == UNIVERSE_CHN:
        have_spread = has_data("_offshore_spread") and has_data("_onshore_spread")
        features[DRIVER_OFFSHORE_SPREAD] = (
            build_chn_offshore_spread(renamed["_offshore_spread"], renamed["_onshore_spread"])
            if have_spread
            else pd.NA
        )

        flow_columns = [f"_flow_{code}" for code in FLOW_BUY_SELL_SERIES + FLOW_TURNOVER_SERIES]
        have_flows = all(has_data(column) for column in flow_columns)
        if have_flows:
            if chn_flows_cutover is None:
                raise ValueError(
                    "CHN flows data is available but chn_flows_cutover was not provided -- "
                    "resolve it once per run (steer.features.resolve_flows_cutover) and pass "
                    "it in, rather than fetch_raw_driver_frame querying metadata per pair."
                )
            cutover = chn_flows_cutover
            spans_cutover = renamed.index.min() < cutover <= renamed.index.max()
            if spans_cutover:
                logger.warning(
                    "chn_flows_window_spans_regime_cutover",
                    series_code=series_code,
                    cutover=str(cutover.date()),
                    note=(
                        "flows mixes net-flows (pre-cutover) and total-turnover "
                        "(post-cutover) regimes -- a rolling estimation window "
                        "straddling this date mixes both for its duration."
                    ),
                )
            codes = FLOW_BUY_SELL_SERIES + FLOW_TURNOVER_SERIES
            features[DRIVER_FLOWS] = build_chn_flows(
                shanghai_buy=renamed[f"_flow_{codes[0]}"],
                shenzhen_buy=renamed[f"_flow_{codes[1]}"],
                shanghai_sell=renamed[f"_flow_{codes[2]}"],
                shenzhen_sell=renamed[f"_flow_{codes[3]}"],
                shanghai_turnover=renamed[f"_flow_{codes[4]}"],
                shenzhen_turnover=renamed[f"_flow_{codes[5]}"],
                cutover=cutover,
            )
        else:
            features[DRIVER_FLOWS] = pd.NA

    return features[[RATE_COLUMN] + list(strategy_config.drivers)]
