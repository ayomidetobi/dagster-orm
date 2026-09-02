"""Dataset-driven pair/driver discovery for STEER, against the real BNP STEER metadata catalog.

Currency pairs come from the datalake via rewrite.data_api.dataset.fx
(FXDevelopedMarkets/FXEmergingMarkets/FXChina) -- see steer/config.py.
Every pair is one non-USD currency vs USD (the catalog has no FX-spot rows
of its own; see fx.py's _STEER_FX_FILTERS docstring for why and how those
rows were added). parse_fx_legs() still parses (base, quote) ISO currency
codes structurally from a pair's series_code -- the `currency` column
can't identify a pair (it holds one value; a pair has two legs).

Every driver leg is resolved as a metadata *filter query* against the real
catalog's controlled vocabulary (sub_asset_class/tenor/market_segment/
currency), not a hand-maintained mnemonic dictionary -- see ROLE_FILTERS
and RoleResolver below. RoleResolver fetches the whole metadata table
*once* (get_metadata() with no filters) and indexes it in memory by
(role, currency), rather than issuing one get_metadata() call per
(role, currency) pair -- with ~195 catalog rows and ~106 distinct
(role, currency) combinations across every universe, that was ~106 real
database round trips per run for a table small enough to just hold in
memory. A role is looked up by currency alone (no market_development
filter): USD's own rate/equity rows are tagged market_development="G10"
in this catalog even though USD is the anchor leg of every EM/CHN pair
too, so filtering role lookups by universe would silently fail to find
them.

Required roles differ by universe (see REQUIRED_ROLES):
  - G10: swap_2y, rate_3m, yield_10y, local_equity, for BOTH legs.
    interest_rate_differential uses swap_2y (both legs); yield_curve_or_cds
    is the (3m - 10y) curve-slope differential, using rate_3m/yield_10y
    (see steer/features.py) -- two different rate drivers, not the same
    series reused twice.
  - EM/CHN: swap_2y and local_equity for BOTH legs; cds_5y for the
    non-USD leg ONLY (yield_curve_or_cds is that leg's CDS *level*, not a
    difference -- see steer/features.py). EM/CHN currently has no
    sovereign-yield coverage in this catalog, so there's no 3m/10y curve
    slope to build for them; cds_5y is the published methodology's driver
    2 for these universes instead.

CHN's cds_5y role resolves to CNHCDS_PX_LAST, a SYNTHETIC PLACEHOLDER (see
its des_notes in meta_series_steer.csv) added on the assumption that CHN
takes the same driver-2 treatment as EM -- the source ticker sheet
supplies no CNH curve legs and no China CDS, so this is unconfirmed. All
catalog data (synthetic or not) is treated as real for resolution
purposes -- is_synthetic is kept only as a deterministic tie-break sort
key (real before synthetic, then series_code) for the rare case where a
role matches more than one row, e.g. CNH's local_equity matching both the
real CNHLIVEMSCI_PX_LAST and the synthetic CNHMSCI_PX_LAST. Nothing
filters on is_synthetic any more.

A pair missing any required role for any required leg is reported blocked
(see PairAvailability.blocked) rather than silently regressed on a
partial/corrupted driver set -- never substitute a global proxy for a
missing per-country input, and never let a pair with missing genuine data
reach estimation.

steer_data_availability (assets/steer/availability_asset.py) does this
resolution once per universe and persists every resolved (leg, role) as a
flat column in its report (build_availability_report); steer_silver_prices
depends on that asset and reconstructs each pair's PairAvailability from
the report row (PairAvailability.from_report_row) instead of re-resolving
from scratch -- the two assets used to duplicate all of this work.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_FX_PAIR_PATTERN = re.compile(r"^([A-Z]{3})([A-Z]{3})_")

_LEGS: Tuple[str, str] = ("base", "quote")


def parse_fx_legs(series_code: str) -> Optional[Tuple[str, str]]:
    """Extract (base, quote) ISO currency codes from an FX series_code like "EURUSD_PX_LAST"."""
    match = _FX_PAIR_PATTERN.match(str(series_code))
    return (match.group(1), match.group(2)) if match else None


#: role name -> the metadata filter (minus `currency`) that resolves it --
#: see RoleResolver. Every driver leg is expressed as a filter query
#: against the real catalog, never a hardcoded series_code list.
ROLE_FILTERS: Dict[str, Dict[str, List[str]]] = {
    "swap_2y": dict(sub_asset_class=["Interest Rate Swap"], tenor=["2Y"]),
    "rate_3m": dict(sub_asset_class=["Money Market Rate"], tenor=["3M"]),
    "yield_10y": dict(sub_asset_class=["Sovereign Yield"], tenor=["10Y"]),
    "cds_5y": dict(sub_asset_class=["Sovereign CDS"], tenor=["5Y"]),
    "local_equity": dict(sub_asset_class=["Equity Index"], market_segment=["Local"]),
}

#: universe -> (roles required for BOTH legs, roles required for the non-USD leg only).
REQUIRED_ROLES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    "G10": (("swap_2y", "rate_3m", "yield_10y", "local_equity"), ()),
    "EM": (("swap_2y", "local_equity"), ("cds_5y",)),
    "CHN": (("swap_2y", "local_equity"), ("cds_5y",)),
}


class RoleResolver:
    """Resolves (role, currency) -> series_code from one in-memory metadata snapshot.

    Replaces ~106 per-(role, currency) queries (one get_metadata() call per
    role/currency combination, across every universe) with a single
    get_metadata() call, indexed here once.
    """

    def __init__(self, metadata: pd.DataFrame) -> None:
        self._index: Dict[Tuple[str, str], pd.DataFrame] = {}
        has_synthetic = "is_synthetic" in metadata.columns
        for role, filters in ROLE_FILTERS.items():
            if not set(filters) <= set(metadata.columns):
                continue  # this role's filter columns aren't in the frame -- no matches
            mask = pd.Series(True, index=metadata.index)
            for column, allowed in filters.items():
                mask &= metadata[column].isin(allowed)
            subset = metadata.loc[mask]
            if subset.empty:
                continue
            # Preserve the original resolve_role()'s tie-break exactly: real
            # before synthetic (False < True), then series_code ascending --
            # this is the ONLY thing is_synthetic still influences (see
            # module docstring). CNH's local_equity depends on it: it
            # matches both the real CNHLIVEMSCI_PX_LAST and the synthetic
            # CNHMSCI_PX_LAST, and series_code alone would pick the real one
            # only by the coincidence that "CNHL" sorts before "CNHM".
            sort_by = ["is_synthetic", "series_code"] if has_synthetic else ["series_code"]
            subset = subset.sort_values(sort_by)
            for currency, group in subset.groupby("currency"):
                self._index[(role, str(currency))] = group

    @classmethod
    def from_data_api(cls, data_api: Any) -> "RoleResolver":
        """Build a resolver from one unfiltered get_metadata() call -- the whole catalog.

        get_metadata() with no arguments is valid (filters defaults to None,
        which resolves to no WHERE clause -- the full table).
        """
        return cls(data_api.get_metadata().frame)

    def resolve(self, role: str, currency: str) -> Tuple[Optional[str], str]:
        """Resolve one (role, currency) to a series_code -- an in-memory lookup, no query.

        Returns (series_code, reason) -- series_code is None (with a "why
        not" reason) if nothing matches.
        """
        group = self._index.get((role, currency))
        if group is None or group.empty:
            return None, f"No {role} series for {currency}."
        series_code = str(group.iloc[0]["series_code"])
        return series_code, f"{role} resolved to {series_code} for {currency}."


def _non_usd_leg(base: str, quote: str) -> Optional[str]:
    """Whichever of base/quote isn't USD -- None if neither (or both) are USD."""
    if base == "USD" and quote != "USD":
        return quote
    if quote == "USD" and base != "USD":
        return base
    return None


@dataclass(frozen=True)
class PairAvailability:
    """Per-pair driver-role availability -- the data_availability report's per-row shape.

    resolved maps (leg, role) -> series_code for every role that
    successfully resolved, leg being "base" or "quote". A driver needing
    several roles/legs (e.g. G10's yield_curve_or_cds needs
    (base, rate_3m), (base, yield_10y), (quote, rate_3m), (quote,
    yield_10y)) reads every one of them out of this single dict -- see
    steer/features.py's fetch_raw_driver_frame.
    """

    series_code: str
    universe: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    resolved: Dict[Tuple[str, str], str] = field(default_factory=dict)
    missing_reasons: Dict[str, str] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        """True if any role required for this universe/pair failed to resolve."""
        return bool(self.missing_reasons)

    @property
    def block_reasons(self) -> List[str]:
        return list(self.missing_reasons.values())

    def get(self, leg: str, role: str) -> Optional[str]:
        """The series_code resolved for (leg, role), or None if it wasn't required/available."""
        return self.resolved.get((leg, role))

    @classmethod
    def from_report_row(cls, row: "pd.Series[Any]") -> "PairAvailability":
        """Rebuild a PairAvailability from one build_availability_report row -- no query.

        Parses the flat `{leg}_{role}` columns back into `resolved` and
        `block_reasons` back into `missing_reasons` (as a semicolon-split,
        synthetically-keyed dict -- the report only persists the reason
        *text*, not the original "{leg}:{role}" keys, so those keys aren't
        recoverable verbatim; `blocked`/`resolved`/`block_reasons` all
        still round-trip exactly, which is what callers actually use).
        """
        resolved: Dict[Tuple[str, str], str] = {}
        for leg in _LEGS:
            for role in ROLE_FILTERS:
                column = f"{leg}_{role}"
                value = row.get(column)
                if value is not None and pd.notna(value):
                    resolved[(leg, role)] = str(value)

        reasons_text = row.get("block_reasons")
        missing_reasons: Dict[str, str] = {}
        if isinstance(reasons_text, str) and reasons_text.strip():
            for index, reason in enumerate(reasons_text.split("; ")):
                missing_reasons[f"reason_{index}"] = reason

        def _optional_str(value: Any) -> Optional[str]:
            return str(value) if value is not None and pd.notna(value) else None

        return cls(
            series_code=str(row["series_code"]),
            universe=str(row["universe"]),
            base_currency=_optional_str(row.get("base_currency")),
            quote_currency=_optional_str(row.get("quote_currency")),
            resolved=resolved,
            missing_reasons=missing_reasons,
        )


def assess_pair_availability(
    series_code: str,
    universe: str,
    resolver: RoleResolver,
) -> PairAvailability:
    """Assess one pair's driver-role availability against `resolver`'s in-memory snapshot.

    Resolves every role REQUIRED_ROLES[universe] calls for -- both legs'
    roles via resolver.resolve(role, base/quote currency), plus the
    non-USD leg's extra roles (e.g. cds_5y) via resolver.resolve(role,
    non_usd_leg) -- and reports the pair blocked if any of them come back
    empty. Pure in-memory lookups: no metadata query happens here at all,
    that already happened once when `resolver` was built.
    """
    legs = parse_fx_legs(series_code)
    base, quote = legs if legs else (None, None)

    if base is None or quote is None:
        reason = f"Could not parse currency legs from series_code {series_code!r}."
        return PairAvailability(
            series_code=series_code,
            universe=universe,
            base_currency=None,
            quote_currency=None,
            missing_reasons={"parse": reason},
        )

    if universe not in REQUIRED_ROLES:
        reason = f"Unknown universe {universe!r} -- no REQUIRED_ROLES entry."
        return PairAvailability(
            series_code=series_code,
            universe=universe,
            base_currency=base,
            quote_currency=quote,
            missing_reasons={"universe": reason},
        )

    both_leg_roles, non_usd_roles = REQUIRED_ROLES[universe]
    resolved: Dict[Tuple[str, str], str] = {}
    missing_reasons: Dict[str, str] = {}

    if non_usd_roles and _non_usd_leg(base, quote) is None:
        # EM/CHN pairs are USD-quoted by construction (see fx.py/the FX
        # rows' des_notes) -- driver 2 there is the non-USD leg's 5Y CDS as
        # a single-country level, which presupposes exactly one non-USD
        # leg. A cross with two non-USD legs (e.g. TRYZAR) has no
        # principled single CDS candidate and no defined driver-2
        # treatment under the published spec -- block outright rather than
        # resolving roles for a pair shape the methodology doesn't cover.
        missing_reasons["non_usd_leg_required"] = (
            "EM and CHN pairs are USD-quoted by construction. EM driver 2 is the non-USD leg's "
            "5Y sovereign CDS as a single-country level; a cross with two non-USD legs has no "
            "defined driver-2 treatment under the published spec."
        )
        return PairAvailability(
            series_code=series_code,
            universe=universe,
            base_currency=base,
            quote_currency=quote,
            resolved=resolved,
            missing_reasons=missing_reasons,
        )

    for leg, currency in (("base", base), ("quote", quote)):
        for role in both_leg_roles:
            code, reason = resolver.resolve(role, currency)
            if code:
                resolved[(leg, role)] = code
            else:
                missing_reasons[f"{leg}:{role}"] = reason

    if non_usd_roles:
        non_usd_currency = _non_usd_leg(base, quote)
        assert non_usd_currency is not None  # guaranteed by the USD-leg check above
        leg = "base" if non_usd_currency == base else "quote"
        for role in non_usd_roles:
            code, reason = resolver.resolve(role, non_usd_currency)
            if code:
                resolved[(leg, role)] = code
            else:
                missing_reasons[f"{leg}:{role}"] = reason

    return PairAvailability(
        series_code=series_code,
        universe=universe,
        base_currency=base,
        quote_currency=quote,
        resolved=resolved,
        missing_reasons=missing_reasons,
    )


def _report_role_columns() -> List[str]:
    """`{leg}_{role}` columns for every role REQUIRED_ROLES references, both legs.

    Derived from ROLE_FILTERS/REQUIRED_ROLES (not hardcoded) so the
    report's schema can never drift from what resolution actually
    produces. A column irrelevant to a given pair (e.g. base_cds_5y --
    every EM/CHN pair in this catalog has USD as the base, so cds_5y only
    ever resolves for the non-USD *quote* leg in practice) is simply
    always null for it, same as any universe that doesn't use a role at
    all (e.g. G10 never populates any *_cds_5y column).
    """
    roles = [
        role
        for role in ROLE_FILTERS
        if any(role in both or role in non_usd for both, non_usd in REQUIRED_ROLES.values())
    ]
    return [f"{leg}_{role}" for leg in _LEGS for role in roles]


def build_availability_report(
    pairs_by_universe: Dict[str, pd.DataFrame],
    data_api: Any,
) -> pd.DataFrame:
    """Build the full data_availability report: one row per pair across every universe.

    pairs_by_universe maps universe name ("G10"/"EM"/"CHN") -> that
    universe's metadata frame (e.g. FXDevelopedMarkets().info).

    Builds exactly ONE RoleResolver (one get_metadata() call) and shares it
    across every pair/universe in this call -- role/currency combos repeat
    heavily (every pair has a USD leg), so resolving all of them from one
    in-memory snapshot instead of a fresh query per (role, currency) is
    what keeps this to a single database round trip regardless of pair
    count. Every resolved (leg, role) is persisted as a flat
    `{leg}_{role}` column (see _report_role_columns) so a downstream
    consumer (steer_silver_prices) can reconstruct each pair's
    PairAvailability via PairAvailability.from_report_row without
    re-resolving anything.
    """
    resolver = RoleResolver.from_data_api(data_api)
    role_columns = _report_role_columns()
    rows = []
    for universe, metadata in pairs_by_universe.items():
        for series_code in metadata["series_code"]:
            availability = assess_pair_availability(series_code, universe, resolver)
            row = {
                "series_code": availability.series_code,
                "universe": availability.universe,
                "base_currency": availability.base_currency,
                "quote_currency": availability.quote_currency,
                "blocked": availability.blocked,
                "block_reasons": "; ".join(availability.block_reasons),
            }
            for leg in _LEGS:
                for role in ROLE_FILTERS:
                    row[f"{leg}_{role}"] = availability.get(leg, role)
            rows.append(row)
    return pd.DataFrame(rows, columns=(
        ["series_code", "universe", "base_currency", "quote_currency", "blocked", "block_reasons"]
        + role_columns
    ))
