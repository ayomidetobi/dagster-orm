"""Shared string constants for the steer/ package -- a leaf module, imports nothing else in
steer/ (see steer/__init__.py's module docstring for the package's import direction).

Centralizes every identifier that would otherwise be a repeated literal string across
config.py/source/discovery.py/source/features.py/analytics/estimation.py/analytics/results.py
(variant names, FX leg names, driver-role names, driver names, signal values, cointegration
modes, ADF parameters, CHN's fixed source series, and DuckLake schema/table names) so a typo
in one spot can't silently diverge from its use elsewhere.

Every `Literal["..."]` constant below is deliberately annotated with its own singleton
Literal type (not just `str`) so it stays assignable wherever the narrower Literal type
(e.g. steer.analytics.estimation.Signal, steer.model.CointegrationMode) is expected -- see
PEP 586, which requires those type aliases to be spelled with literal string syntax rather
than a name, so they can't reference these constants directly.

Numeric tuning knobs (window sizes, tolerances, sanity bounds) intentionally stay next to
the logic they tune (source/features.py, analytics/results.py) -- those aren't "magic
strings," and centralizing them here would separate them from the docstring explaining the
specific number chosen.
"""

from __future__ import annotations

from typing import Literal, Tuple

# --- Variants --------------------------------------------------------------------------
VARIANT_G10: Literal["G10"] = "G10"
VARIANT_EM: Literal["EM"] = "EM"
VARIANT_CHN: Literal["CHN"] = "CHN"
VARIANTS: Tuple[str, ...] = (VARIANT_G10, VARIANT_EM, VARIANT_CHN)

#: The anchor currency of every EM/CHN pair (always USD-quoted by construction -- see
#: steer/source/discovery.py's module docstring) and of most G10 crosses.
CURRENCY_USD: Literal["USD"] = "USD"

# --- FX pair legs (steer/source/discovery.py's PairAvailability.resolved keys) -----------
LEG_BASE: Literal["base"] = "base"
LEG_QUOTE: Literal["quote"] = "quote"
LEGS: Tuple[str, str] = (LEG_BASE, LEG_QUOTE)

# --- Driver roles (steer/source/discovery.py's ROLE_FILTERS/REQUIRED_ROLES) --------------
ROLE_SWAP_2Y: Literal["swap_2y"] = "swap_2y"
ROLE_RATE_3M: Literal["rate_3m"] = "rate_3m"
ROLE_YIELD_10Y: Literal["yield_10y"] = "yield_10y"
ROLE_CDS_5Y: Literal["cds_5y"] = "cds_5y"
ROLE_LOCAL_EQUITY: Literal["local_equity"] = "local_equity"

# --- Driver names (steer/config.py's DRIVER_NAMES; source/features.py's feature columns) -
DRIVER_INTEREST_RATE_DIFFERENTIAL: Literal["interest_rate_differential"] = (
    "interest_rate_differential"
)
DRIVER_YIELD_CURVE_OR_CDS: Literal["yield_curve_or_cds"] = "yield_curve_or_cds"
DRIVER_LOCAL_EQUITY: Literal["local_equity"] = "local_equity"
DRIVER_GLOBAL_EQUITY: Literal["global_equity"] = "global_equity"
DRIVER_COMMODITY: Literal["commodity"] = "commodity"
#: CHN-only extra drivers, on top of the 5 canonical ones above -- see
#: steer/config.py's FX_CHN and steer/source/features.py's module docstring.
DRIVER_OFFSHORE_SPREAD: Literal["offshore_spread"] = "offshore_spread"
DRIVER_FLOWS: Literal["flows"] = "flows"

#: STEER's 5 canonical drivers, in the fixed order every variant's expected_signs/gold
#: tables use -- see steer/config.py's StrategyConfig.drivers.
DRIVER_NAMES: Tuple[str, ...] = (
    DRIVER_INTEREST_RATE_DIFFERENTIAL,
    DRIVER_YIELD_CURVE_OR_CDS,
    DRIVER_LOCAL_EQUITY,
    DRIVER_GLOBAL_EQUITY,
    DRIVER_COMMODITY,
)

# --- steer_features/steer_estimates gold-layer column names (source/features.py) --------
RATE_COLUMN: Literal["rate"] = "rate"
REALIZED_VOLATILITY_COLUMN: Literal["realized_volatility"] = "realized_volatility"
IS_LOGGED_COLUMN: Literal["is_logged"] = "is_logged"
#: The column source/features.py's build_silver_frame tags each pair's rows with in the
#: combined silver frame.
SERIES_CODE_COLUMN: Literal["series_code"] = "series_code"

# --- Signals (analytics/estimation.py's Signal literal) ---------------------------------
SIGNAL_BUY: Literal["BUY"] = "BUY"
SIGNAL_SELL: Literal["SELL"] = "SELL"
SIGNAL_NONE: Literal["NONE"] = "NONE"

# --- Steer.fit()'s cointegration mode (steer/model.py's CointegrationMode literal) -------
COINTEGRATION_MODE_LATEST: Literal["latest"] = "latest"
COINTEGRATION_MODE_EACH: Literal["each"] = "each"

# --- Engle-Granger/ADF parameters, matching production exactly (see
# --- analytics/estimation.py's module docstring for why these exact values) --------------
ADF_REGRESSION_TYPE: Literal["c"] = "c"
ADF_AUTOLAG_CRITERION: Literal["BIC"] = "BIC"
ADF_CRITICAL_VALUE_LEVELS: Tuple[str, ...] = ("1%", "5%", "10%")

# --- CHN's fixed offshore/flows source series (source/features.py) ----------------------
OFFSHORE_SPREAD_SERIES: str = "OFFSHORE_SPREAD_PX_LAST"
ONSHORE_SPREAD_SERIES: str = "ONSHORE_SPREAD_PX_LAST"
FLOW_BUY_SELL_SERIES: Tuple[str, ...] = (
    "SHANGHAI_BUY_FLOWS_PX_LAST",
    "SHENZHEN_BUY_FLOWS_PX_LAST",
    "SHANGHAI_SELL_FLOWS_PX_LAST",
    "SHENZHEN_SELL_FLOWS_PX_LAST",
)
FLOW_TURNOVER_SERIES: Tuple[str, ...] = (
    "SHANGHAI_FLOWS_TURNOVER_PX_LAST",
    "SHENZHEN_FLOWS_TURNOVER_PX_LAST",
)

# --- DuckLake silver/gold schema + table names (steer/orm.py) ---------------------------
SILVER_SCHEMA: Literal["silver"] = "silver"
GOLD_SCHEMA: Literal["gold"] = "gold"
STEER_ESTIMATES_TABLE: Literal["steer_estimates"] = "steer_estimates"
STEER_SIGNALS_TABLE: Literal["steer_signals"] = "steer_signals"
#: SteerResult's 2 tables -- see steer/analytics/results.py's module docstring. steer_results is
#: long-form (one row per series_code/as_of/date); steer_result_summary is one row per
#: series_code/as_of (z_score, upper/lower, and every coefficient/standard_error/p_value,
#: flattened).
STEER_RESULTS_TABLE: Literal["steer_results"] = "steer_results"
STEER_RESULT_SUMMARY_TABLE: Literal["steer_result_summary"] = "steer_result_summary"
