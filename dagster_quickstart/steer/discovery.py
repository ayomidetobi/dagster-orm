"""Dataset-driven pair/driver discovery for STEER, replacing the old YAML variable_set.

Currency pairs and their rate series now come straight from the datalake
via rewrite.data_api.dataset.fx (FXDevelopedMarkets/FXEmergingMarkets/
FXChina), not hand-typed in YAML -- see steer/config.py.

Two of the 5 STEER drivers can only be genuinely sourced per-pair from
this catalog's real metadata:
  - global_equity, commodity: single series, config-provided (YAML) --
    these are supposed to be one global benchmark applied identically to
    every pair, so there's nothing to "discover" per pair.
  - local_equity: needs a real per-country/per-currency equity index for
    BOTH of a pair's currencies. Most of this catalog's Equity metadata
    (Common Stock / generic "Regional Index" rows) has no per-row country
    signal at all -- but 14 real per-currency MSCI index series were added
    explicitly for this (AUD, CNY, EUR, GBP, INR, JPY, MXN, NOK, RUB, SAR,
    SEK, SGD, USD, ZAR -- see EQUITY_SERIES_TO_CURRENCY). A pair is only
    "local equity available" if both legs are in that covered set; any
    other currency (there is currently no broader coverage) is honestly
    reported unavailable, never a fabricated proxy.
  - interest_rate_differential / yield_curve_or_cds: both need a real
    sovereign-yield or interest-rate-swap series for BOTH of a pair's
    currencies. This catalog's Fixed Income data covers 9 currencies (AUD,
    CAD, CHF, EUR, GBP, JPY, NOK, SEK, USD -- sovereign yields via 8
    countries, where DE/FR/IT all map to EUR, plus the 2Y swap series in
    SWAP_SERIES_TO_CURRENCY, CHF/NOK/SEK's only source). A pair is only
    "rate data available" if both legs are in that covered set.
    NKSW2_PX_LAST/SKSW2_PX_LAST (NOK/SEK) are placeholder demo tickers, not
    verified real Bloomberg mnemonics -- added on explicit request to
    unblock those currencies in this demo catalog, unlike the other swap
    entries (which are real tickers the requester supplied).

A pair missing either local_equity or rate data is explicitly reported as
blocked (see assess_pair_availability) rather than silently regressed on
a partial/corrupted driver set -- this was a direct instruction, not a
judgment call: never substitute a global proxy for a missing per-country
input, and never let a pair with missing genuine data reach estimation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

#: Country code (from a Fixed Income series_code's prefix, e.g. "US2Y_YIELD_0021")
#: -> the currency that country's sovereign yield is relevant to.
COUNTRY_TO_CURRENCY: Dict[str, str] = {
    "US": "USD",
    "UK": "GBP",
    "JP": "JPY",
    "DE": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "CA": "CAD",
    "AU": "AUD",
}

#: 2Y interest-rate-swap series_code -> currency. Unlike sovereign-yield
#: series_codes (parsed structurally via _FI_COUNTRY_PATTERN), these are
#: vendor mnemonics with no derivable country-prefix structure -- listed
#: explicitly rather than guessed at with a regex. CHF/NOK/SEK have no
#: sovereign-yield series in this catalog at all, so their swap entry is
#: currently their only rate-data source. EUSA2/USOSFR2/BPSW2/JYSO2/SFSW2/
#: ADSW2 are real Bloomberg mnemonics supplied by the catalog owner;
#: NKSW2/SKSW2 are placeholder demo tickers (not verified real), added on
#: explicit request just to unblock NOK/SEK in this demo catalog.
SWAP_SERIES_TO_CURRENCY: Dict[str, str] = {
    "EUSA2_PX_LAST": "EUR",
    "USOSFR2_PX_LAST": "USD",
    "BPSW2_PX_LAST": "GBP",
    "JYSO2_PX_LAST": "JPY",
    "SFSW2_PX_LAST": "CHF",
    "ADSW2_PX_LAST": "AUD",
    "NKSW2_PX_LAST": "NOK",
    "SKSW2_PX_LAST": "SEK",
}

#: Local-equity-index series_code -> currency. Same reasoning as
#: SWAP_SERIES_TO_CURRENCY: these are real per-currency MSCI index
#: mnemonics (e.g. "AUD_PX_LAST" -> MXAU Index) with no structure to parse
#: generically -- listed explicitly. Everything else in this catalog's
#: Equity metadata (Common Stock / generic "Regional Index" rows) carries
#: no country/currency signal at all and is never matched here.
EQUITY_SERIES_TO_CURRENCY: Dict[str, str] = {
    "AUD_PX_LAST": "AUD",
    "CNY_PX_LAST": "CNY",
    "EUR_PX_LAST": "EUR",
    "GBP_PX_LAST": "GBP",
    "INR_PX_LAST": "INR",
    "JPY_PX_LAST": "JPY",
    "MXN_PX_LAST": "MXN",
    "NOK_PX_LAST": "NOK",
    "RUB_PX_LAST": "RUB",
    "SAR_PX_LAST": "SAR",
    "SEK_PX_LAST": "SEK",
    "SGD_PX_LAST": "SGD",
    "USD_PX_LAST": "USD",
    "ZAR_PX_LAST": "ZAR",
}

_FX_PAIR_PATTERN = re.compile(r"^([A-Z]{3})([A-Z]{3})_SPOT")
_FI_COUNTRY_PATTERN = re.compile(r"^([A-Z]{2,3})\d")


def parse_fx_legs(series_code: str) -> Optional[Tuple[str, str]]:
    """Extract (base, quote) ISO currency codes from an FX series_code like "AUDJPY_SPOT_0004"."""
    match = _FX_PAIR_PATTERN.match(str(series_code))
    return (match.group(1), match.group(2)) if match else None


def parse_fi_country(series_code: str) -> Optional[str]:
    """Extract the country-code prefix from a Fixed Income series_code like "US2Y_YIELD_0021"."""
    match = _FI_COUNTRY_PATTERN.match(str(series_code))
    return match.group(1) if match else None


def parse_fi_currency(series_code: str) -> Optional[str]:
    """Resolve a Fixed Income series_code to a currency, sovereign yield or swap.

    Tries the explicit swap mnemonic table first (SWAP_SERIES_TO_CURRENCY),
    then falls back to the country-prefix parse (parse_fi_country +
    COUNTRY_TO_CURRENCY) that sovereign-yield series_codes follow.
    """
    currency = SWAP_SERIES_TO_CURRENCY.get(str(series_code))
    if currency:
        return currency
    country = parse_fi_country(series_code)
    return COUNTRY_TO_CURRENCY.get(country) if country else None


def build_currency_to_fi_series(fixed_income_metadata: pd.DataFrame) -> Dict[str, List[str]]:
    """Map currency -> every Fixed Income series_code covering it (via parse_fi_currency).

    A currency can have several series (different tenors, or a sovereign
    yield alongside a swap) -- callers pick whichever they need; this just
    answers "is there anything at all".
    """
    by_currency: Dict[str, List[str]] = {}
    for series_code in fixed_income_metadata["series_code"]:
        currency = parse_fi_currency(series_code)
        if currency:
            by_currency.setdefault(currency, []).append(series_code)
    return by_currency


def parse_equity_currency(series_code: str) -> Optional[str]:
    """Resolve an Equity series_code to a currency, via the explicit EQUITY_SERIES_TO_CURRENCY table."""
    return EQUITY_SERIES_TO_CURRENCY.get(str(series_code))


def build_currency_to_equity_series(equity_metadata: pd.DataFrame) -> Dict[str, List[str]]:
    """Map currency -> every local-equity-index series_code covering it (via parse_equity_currency).

    Mirrors build_currency_to_fi_series -- callers pass this catalog's
    Equity metadata broadly (most rows won't match EQUITY_SERIES_TO_CURRENCY
    and are silently skipped, same as build_currency_to_fi_series ignoring
    non-Fixed-Income-shaped codes).
    """
    by_currency: Dict[str, List[str]] = {}
    for series_code in equity_metadata["series_code"]:
        currency = parse_equity_currency(series_code)
        if currency:
            by_currency.setdefault(currency, []).append(series_code)
    return by_currency


@dataclass(frozen=True)
class PairAvailability:
    """Per-pair driver availability -- the data_availability report's per-row shape."""

    series_code: str
    universe: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    local_equity_available: bool
    local_equity_reason: str
    rate_data_available: bool
    rate_data_reason: str
    base_rate_series: Optional[str] = None
    quote_rate_series: Optional[str] = None
    base_equity_series: Optional[str] = None
    quote_equity_series: Optional[str] = None

    @property
    def blocked(self) -> bool:
        """True if this pair is missing a genuine per-country input for any driver."""
        return not (self.local_equity_available and self.rate_data_available)

    @property
    def block_reasons(self) -> List[str]:
        reasons = []
        if not self.local_equity_available:
            reasons.append(self.local_equity_reason)
        if not self.rate_data_available:
            reasons.append(self.rate_data_reason)
        return reasons


def _coverage_reason(
    *,
    driver_label: str,
    base: str,
    quote: str,
    base_series: List[str],
    quote_series: List[str],
) -> str:
    """Shared "found coverage for both legs" / "missing for: X, Y" reason text.

    Used identically for rate_data and local_equity -- both are "both legs
    need a matching series" checks against a currency -> series_code map.
    """
    if base_series and quote_series:
        return f"{driver_label} coverage found for both {base} and {quote}."
    missing = [ccy for ccy, series in ((base, base_series), (quote, quote_series)) if not series]
    return f"No {driver_label.lower()} series in this catalog for: {', '.join(missing)}."


def assess_pair_availability(
    series_code: str,
    universe: str,
    *,
    currency_to_fi_series: Dict[str, List[str]],
    currency_to_equity_series: Dict[str, List[str]],
) -> PairAvailability:
    """Assess one pair's driver availability against the real catalog.

    local_equity is available only if both legs' currencies have a
    per-currency equity index in currency_to_equity_series (see
    EQUITY_SERIES_TO_CURRENCY) -- pass {} to report every pair as
    local_equity-unavailable. rate_data (interest_rate_differential /
    yield_curve_or_cds) is available only if both legs' currencies have
    Fixed Income coverage.
    """
    legs = parse_fx_legs(series_code)
    base, quote = legs if legs else (None, None)

    if base is None or quote is None:
        unparsed_reason = f"Could not parse currency legs from series_code {series_code!r}."
        return PairAvailability(
            series_code=series_code,
            universe=universe,
            base_currency=None,
            quote_currency=None,
            local_equity_available=False,
            local_equity_reason=unparsed_reason,
            rate_data_available=False,
            rate_data_reason=unparsed_reason,
        )

    base_rate_series = currency_to_fi_series.get(base, [])
    quote_rate_series = currency_to_fi_series.get(quote, [])
    rate_data_available = bool(base_rate_series) and bool(quote_rate_series)
    rate_data_reason = _coverage_reason(
        driver_label="Sovereign-yield or interest-rate-swap",
        base=base,
        quote=quote,
        base_series=base_rate_series,
        quote_series=quote_rate_series,
    )

    base_equity_series = currency_to_equity_series.get(base, [])
    quote_equity_series = currency_to_equity_series.get(quote, [])
    local_equity_available = bool(base_equity_series) and bool(quote_equity_series)
    local_equity_reason = _coverage_reason(
        driver_label="Local-equity-index",
        base=base,
        quote=quote,
        base_series=base_equity_series,
        quote_series=quote_equity_series,
    )

    return PairAvailability(
        series_code=series_code,
        universe=universe,
        base_currency=base,
        quote_currency=quote,
        local_equity_available=local_equity_available,
        local_equity_reason=local_equity_reason,
        rate_data_available=rate_data_available,
        rate_data_reason=rate_data_reason,
        base_rate_series=base_rate_series[0] if base_rate_series else None,
        quote_rate_series=quote_rate_series[0] if quote_rate_series else None,
        base_equity_series=base_equity_series[0] if base_equity_series else None,
        quote_equity_series=quote_equity_series[0] if quote_equity_series else None,
    )


def build_availability_report(
    pairs_by_universe: Dict[str, pd.DataFrame],
    fixed_income_metadata: pd.DataFrame,
    equity_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Build the full data_availability report: one row per pair across every universe.

    pairs_by_universe maps universe name ("G10"/"EM"/"CHN") -> that
    universe's metadata frame (e.g. FXDevelopedMarkets().info).
    """
    currency_to_fi_series = build_currency_to_fi_series(fixed_income_metadata)
    currency_to_equity_series = build_currency_to_equity_series(equity_metadata)

    rows = []
    for universe, metadata in pairs_by_universe.items():
        for series_code in metadata["series_code"]:
            availability = assess_pair_availability(
                series_code,
                universe,
                currency_to_fi_series=currency_to_fi_series,
                currency_to_equity_series=currency_to_equity_series,
            )
            rows.append(
                {
                    "series_code": availability.series_code,
                    "universe": availability.universe,
                    "base_currency": availability.base_currency,
                    "quote_currency": availability.quote_currency,
                    "local_equity_available": availability.local_equity_available,
                    "rate_data_available": availability.rate_data_available,
                    "blocked": availability.blocked,
                    "block_reasons": "; ".join(availability.block_reasons),
                }
            )
    return pd.DataFrame(rows)
