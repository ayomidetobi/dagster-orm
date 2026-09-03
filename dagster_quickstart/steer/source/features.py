"""Data in for STEER: fetch a variant's pairs, conform them onto a shared calendar, and build
the gold-layer feature table each pair is estimated on.

Three steps, in the order they run:

1. `conform_to_business_days` reindexes a pair's raw rate + driver series (bronze data, whatever
   timestamps each vendor happened to report) onto one shared business-day calendar, short gaps
   forward-filled.
2. `build_steer_features` (with `fetch_raw_driver_frame`/`DriverValues` feeding it this variant's
   raw rate + driver series) turns the conformed frame into the per-pair feature table: this
   variant's drivers (StrategyConfig.drivers -- 5 for G10/EM, 7 for CHN) plus a rolling
   `is_logged` flag, one row per date.
3. `build_silver_frame` orchestrates both across every pair in a variant in one call: collect
   every pair's needed series upfront (`required_series_codes`) and fetch them all in a single
   `DriverValues.load()`, skip pairs that are blocked or stale, and concatenate the rest into
   one `series_code`-tagged frame (`SilverResult`).

Driver construction is deliberately NOT symmetric between G10 and EM/CHN -- this follows the
published STEER methodology, not a simplification:

  - interest_rate_differential: base_swap_2y - quote_swap_2y, every variant.
  - yield_curve_or_cds: G10 is a genuine curve-slope differential,
    (base_3m - base_10y) - (quote_3m - quote_10y); EM/CHN is the non-USD
    leg's 5Y sovereign CDS *level* (not a difference -- EM/CHN pairs are
    always vs. USD, and USD has no CDS quote of its own in this catalog).
  - local_equity: G10 is log(base_msci) - log(quote_msci); EM/CHN is
    log(non_usd_msci) alone (single leg, same USD-quote reasoning as CDS).
  - global_equity / commodity: log(single global series), identical
    across every pair/variant (see steer/config.py's GLOBAL_DRIVERS).

Every log/differential input is read out of a pair's PairAvailability (steer/source/discovery.py)
-- resolved *by role*, never a hardcoded series_code. A driver missing any input it needs is
filled with pd.NA for that pair, never substituted with a proxy (e.g. the global equity series
standing in for a missing local_equity) -- see fetch_raw_driver_frame.

CHN also gets two extra drivers (offshore_spread, flows) -- see build_chn_offshore_spread/
build_chn_flows below and steer/config.py's FX_CHN for why they're not in the 5 canonical
DRIVER_NAMES.

A pair whose bronze data isn't fresh as of the run date, or that's blocked at the discovery
stage (missing genuine per-country data -- see steer/source/discovery.py), is skipped rather
than passed through partial; skipping never raises, the caller decides what to do with an empty
result (see assess_freshness, build_silver_frame).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import structlog

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
    SERIES_CODE_COLUMN,
    VARIANT_CHN,
    VARIANT_G10,
)
from dagster_quickstart.steer.source.discovery import PairAvailability

if TYPE_CHECKING:
    # source/ needs StrategyConfig only for a type annotation and .drivers/.global_equity_series/
    # .commodity_series -- a module-level import would be an upward reference (config.py sits
    # above source/ in the import direction: constants, errors -> source/ -> analytics/ ->
    # config, orm, model -> run). TYPE_CHECKING-only keeps full type checking with no runtime
    # import, so this stays a real leaf at import time.
    from dagster_quickstart.steer.config import StrategyConfig

logger = structlog.get_logger(__name__)


#: Silver-layer calendar conforming -- step one of feature building, and its only caller is the
#: feature/silver-pipeline path below, so it lives here rather than its own module.
MAX_FORWARD_FILL_DAYS = 3


def conform_to_business_days(
    raw: pd.DataFrame,
    *,
    max_forward_fill_days: int = MAX_FORWARD_FILL_DAYS,
    primary_column: str | None = None,
) -> pd.DataFrame:
    """Reindex `raw` (DatetimeIndex, one column per series) onto a business-day calendar.

    A short gap (a real holiday, a vendor's one-off missing day) is
    forward-filled up to `max_forward_fill_days` -- the last known value
    carries forward, which is standard practice for a market that simply
    didn't trade that day. A gap longer than that is left as NaN rather
    than silently carrying a stale price forward indefinitely; downstream,
    estimate_steer's window .dropna() naturally excludes those rows.

    `raw`'s index already reflects the union of every fetched series'
    dates (see fetch_raw_driver_frame below) -- one driver with a
    much longer real history than the rate itself (e.g. a global benchmark
    ingested back to 1970, vs. a pair only 90 days old) would otherwise
    stretch the calendar back decades before the rate even starts,
    producing a flood of leading rate=NaN rows. Pass primary_column (e.g.
    the rate column) to bound the calendar to just that column's own
    non-null range instead of the whole frame's; omit it to use the whole
    frame's range, as before.
    """
    if raw.empty:
        return raw

    bounds_source = raw[primary_column].dropna() if primary_column else raw
    if bounds_source.empty:
        return raw.iloc[0:0]

    business_days = pd.bdate_range(bounds_source.index.min(), bounds_source.index.max())
    conformed = raw.reindex(business_days)
    conformed = conformed.ffill(limit=max_forward_fill_days)
    conformed.index.name = raw.index.name
    return conformed


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
    (see assets/steer/silver_asset.py). `drivers` is this pair's variant's
    driver set (StrategyConfig.drivers -- 5 for G10/EM, 7 for CHN), not a
    fixed module constant, so CHN's extra offshore_spread/flows columns
    survive instead of being silently dropped. Adds realized_volatility and
    is_logged (recomputed per day, so a pair can cross the log/level
    threshold over time -- estimate_steer/cointegration_test are told which
    regime applies for the specific `as_of` they're evaluating, via that
    day's is_logged value).

    Raises KeyError if `raw` is missing RATE_COLUMN or any of `drivers` --
    validated more thoroughly downstream by steer.analytics.results.steer_features_schema
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
    strategy_config: "StrategyConfig",
    availability: PairAvailability,
) -> Dict[str, str]:
    """The column-name -> series_code mapping this pair needs (rate + global drivers +
    whatever driver-role series `availability` resolved, plus CHN's fixed offshore_spread/
    flows series). Extracted out of fetch_raw_driver_frame so required_series_codes() can
    collect every pair's needed series upfront without duplicating this logic.
    """
    variant = strategy_config.variant
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

    if variant == VARIANT_G10:
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

    if variant == VARIANT_CHN:
        series_by_column["_offshore_spread"] = OFFSHORE_SPREAD_SERIES
        series_by_column["_onshore_spread"] = ONSHORE_SPREAD_SERIES
        for code in FLOW_BUY_SELL_SERIES + FLOW_TURNOVER_SERIES:
            series_by_column[f"_flow_{code}"] = code

    return series_by_column


def required_series_codes(
    pairs: Iterable[Tuple[str, PairAvailability]],
    strategy_config: "StrategyConfig",
) -> set:
    """Every series_code fetch_raw_driver_frame would need across all of `pairs`
    (series_code, availability) -- for DriverValues.load(), so one variant's whole run
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
    strategy_config: "StrategyConfig",
    availability: PairAvailability,
    *,
    start: Any = None,
    end: Any = None,
    chn_flows_cutover: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Build one pair's rate + variant drivers (strategy_config.drivers columns) from an already-loaded DriverValues.

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
    per run (resolve_flows_cutover above) and pass it in, rather
    than this function doing its own get_metadata() call per pair.
    """
    variant = strategy_config.variant
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

        DriverValues.select() always creates a column for every requested mapping entry, even
        one with zero real rows (filled with pd.NA) -- so a plain "column in renamed" check
        would always be True here regardless of whether real data was actually returned.
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

    if variant == VARIANT_G10:
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

    if variant == VARIANT_CHN:
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
                    "resolve it once per run (resolve_flows_cutover above) and pass "
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


#: A pair is "fresh" if its rate series has a value within this many days of
#: `as_of` -- generous enough to tolerate a weekend/holiday without
#: false-failing every Monday, but tight enough to catch upstream ingestion
#: actually having stopped.
FRESHNESS_TOLERANCE_DAYS = 3


def assess_freshness(
    raw: Optional[pd.DataFrame],
    *,
    as_of: pd.Timestamp,
    tolerance_days: int = FRESHNESS_TOLERANCE_DAYS,
) -> Tuple[bool, str]:
    """(is_fresh, reason) for one pair's raw frame.

    raw=None or empty means nothing was fetched at all -- always stale
    (there's no bronze data for this pair yet).
    """
    if raw is None or raw.empty:
        return False, "no bronze data"

    latest_timestamp = raw.index.max()
    age_days = (as_of - latest_timestamp).days
    if age_days <= tolerance_days:
        return True, f"{age_days}d old"
    return False, f"{age_days}d old (tolerance {tolerance_days}d)"


@dataclass(frozen=True)
class SilverResult:
    """Everything the silver asset needs to emit, computed without Dagster.

    blocked_pairs/stale_pairs are the series_code (and, for stale, "CODE (reason)") lists the
    asset's AssetCheckResult/Output metadata reports. skipped_reasons is series_code ->
    "blocked: <reasons>" for every BLOCKED pair only -- stale pairs are counted in stale_pairs
    but not logged individually, so they're not in here. chn_flows_cutover_error is set instead
    of logging a warning directly, when this is a CHN variant and resolve_flows_cutover() failed.
    """

    frame: pd.DataFrame
    pair_count: int
    fetched_pair_count: int
    blocked_pairs: List[str] = field(default_factory=list)
    stale_pairs: List[str] = field(default_factory=list)
    skipped_reasons: Dict[str, str] = field(default_factory=dict)
    chn_flows_cutover_error: Optional[str] = None


def build_silver_frame(
    data_api: Any,
    variant: str,
    strategy_config: "StrategyConfig",
    availabilities: Sequence[PairAvailability],
    *,
    as_of: pd.Timestamp,
) -> SilverResult:
    """Fetch every pair's rate + drivers and conform them onto a business-day calendar.

    `availabilities` is one variant's PairAvailability per pair (e.g. from
    steer.source.discovery.pairs_from_availability_report). A pair with
    availability.blocked (missing genuine per-country data for local_equity
    or the rate-based drivers -- see steer/source/discovery.py's module docstring)
    is skipped and never fetched further; a pair whose bronze data isn't
    fresh as of `as_of` (see assess_freshness) is skipped too. Skipping one
    pair never raises -- the caller decides what to do with an empty result.

    Values are fetched ONCE for the whole variant, not once per pair (see
    DriverValues above) -- required_series_codes() collects every
    pair's needed series upfront (including blocked pairs -- cheap, and
    simpler than tracking which ones to skip) and DriverValues.load()
    fetches them all in one call; fetch_raw_driver_frame then only slices
    columns out of that already-loaded frame, per pair, in memory.
    """
    all_series_codes = required_series_codes(
        ((availability.series_code, availability) for availability in availabilities),
        strategy_config,
    )
    driver_values = DriverValues.load(data_api, all_series_codes)

    chn_flows_cutover = None
    chn_flows_cutover_error = None
    if variant == VARIANT_CHN:
        try:
            chn_flows_cutover = resolve_flows_cutover(data_api)
        except ValueError as exc:
            # Resolved once, upfront, rather than per pair -- but tolerantly: a metadata
            # hiccup here shouldn't fail every OTHER CHN driver too. fetch_raw_driver_frame
            # raises its own clear error later, only if a pair's flows data actually needs it.
            chn_flows_cutover_error = str(exc)

    conformed_frames = []
    blocked_pairs: List[str] = []
    stale_pairs: List[str] = []
    skipped_reasons: Dict[str, str] = {}

    for availability in availabilities:
        if availability.blocked:
            blocked_pairs.append(availability.series_code)
            skipped_reasons[availability.series_code] = f"blocked: {'; '.join(availability.block_reasons)}"
            continue

        raw = fetch_raw_driver_frame(
            driver_values,
            availability.series_code,
            strategy_config,
            availability,
            chn_flows_cutover=chn_flows_cutover,
        )
        is_fresh, reason = assess_freshness(raw, as_of=as_of)
        if not is_fresh:
            stale_pairs.append(f"{availability.series_code} ({reason})")
            continue

        conformed = conform_to_business_days(raw, primary_column=RATE_COLUMN).copy()
        conformed[SERIES_CODE_COLUMN] = availability.series_code
        conformed_frames.append(conformed)

    combined = pd.concat(conformed_frames) if conformed_frames else pd.DataFrame()

    return SilverResult(
        frame=combined,
        pair_count=len(availabilities),
        fetched_pair_count=len(conformed_frames),
        blocked_pairs=blocked_pairs,
        stale_pairs=stale_pairs,
        skipped_reasons=skipped_reasons,
        chn_flows_cutover_error=chn_flows_cutover_error,
    )
