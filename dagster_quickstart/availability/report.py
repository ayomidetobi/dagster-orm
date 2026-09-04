"""PairAvailability / assess_pair_availability / build_availability_report.

Every driver leg is resolved as a metadata *filter query* against the real catalog's controlled
vocabulary, via RoleResolver -- not a hand-maintained mnemonic dictionary. Which roles a variant
needs, and whether it requires exactly one non-USD leg, comes from the AvailabilitySpec passed
to every function here; this module has no opinion on either.

A pair missing any role its spec requires for any required leg is reported blocked (see
PairAvailability.blocked) rather than silently regressed on a partial/corrupted driver set --
never substitute a global proxy for a missing per-country input, and never let a pair with
missing genuine data reach estimation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from dagster_quickstart.availability.pairs import (
    LEG_BASE,
    LEG_QUOTE,
    LEGS,
    non_usd_leg,
    parse_fx_legs,
)
from dagster_quickstart.availability.roles import RoleResolver
from dagster_quickstart.availability.spec import AvailabilitySpec


@dataclass(frozen=True)
class PairAvailability:
    """Per-pair driver-role availability -- the data_availability report's per-row shape.

    resolved maps (leg, role) -> series_code for every role that
    successfully resolved, leg being "base" or "quote". A driver needing
    several roles/legs reads every one of them out of this single dict.
    """

    series_code: str
    variant: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    resolved: Dict[Tuple[str, str], str] = field(default_factory=dict)
    missing_reasons: Dict[str, str] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        """True if any role required for this variant/pair failed to resolve."""
        return bool(self.missing_reasons)

    @property
    def block_reasons(self) -> List[str]:
        return list(self.missing_reasons.values())

    def get(self, leg: str, role: str) -> Optional[str]:
        """The series_code resolved for (leg, role), or None if it wasn't required/available."""
        return self.resolved.get((leg, role))

    @classmethod
    def from_report_row(cls, row: "pd.Series[Any]", spec: AvailabilitySpec) -> "PairAvailability":
        """Rebuild a PairAvailability from one build_availability_report row -- no query.

        Parses the flat `{leg}_{role}` columns back into `resolved` and
        `block_reasons` back into `missing_reasons` (as a semicolon-split,
        synthetically-keyed dict -- the report only persists the reason
        *text*, not the original "{leg}:{role}" keys, so those keys aren't
        recoverable verbatim; `blocked`/`resolved`/`block_reasons` all
        still round-trip exactly, which is what callers actually use).
        """
        resolved: Dict[Tuple[str, str], str] = {}
        for leg in LEGS:
            for role in spec.role_filters:
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
            variant=str(row["variant"]),
            base_currency=_optional_str(row.get("base_currency")),
            quote_currency=_optional_str(row.get("quote_currency")),
            resolved=resolved,
            missing_reasons=missing_reasons,
        )


def assess_pair_availability(
    series_code: str,
    variant: str,
    resolver: RoleResolver,
    spec: AvailabilitySpec,
) -> PairAvailability:
    """Assess one pair's driver-role availability against `resolver`'s in-memory snapshot.

    Resolves every role spec.required_roles[variant] calls for -- both legs'
    roles via resolver.resolve(role, base/quote currency), plus the
    non-USD leg's extra roles via resolver.resolve(role, non_usd_leg) --
    and reports the pair blocked if any of them come back empty. Pure
    in-memory lookups: no metadata query happens here at all, that already
    happened once when `resolver` was built.
    """
    legs = parse_fx_legs(series_code)
    base, quote = legs if legs else (None, None)

    if base is None or quote is None:
        reason = f"Could not parse currency legs from series_code {series_code!r}."
        return PairAvailability(
            series_code=series_code,
            variant=variant,
            base_currency=None,
            quote_currency=None,
            missing_reasons={"parse": reason},
        )

    if variant not in spec.required_roles:
        reason = f"Unknown variant {variant!r} -- no required_roles entry."
        return PairAvailability(
            series_code=series_code,
            variant=variant,
            base_currency=base,
            quote_currency=quote,
            missing_reasons={"variant": reason},
        )

    both_leg_roles, non_usd_roles = spec.required_roles[variant]
    resolved: Dict[Tuple[str, str], str] = {}
    missing_reasons: Dict[str, str] = {}

    if spec.single_non_usd_leg.get(variant, False) and non_usd_leg(base, quote) is None:
        # A pair shape the methodology doesn't cover -- block outright rather than resolving
        # roles for it. See AvailabilitySpec.single_non_usd_leg_reason.
        missing_reasons["non_usd_leg_required"] = spec.single_non_usd_leg_reason
        return PairAvailability(
            series_code=series_code,
            variant=variant,
            base_currency=base,
            quote_currency=quote,
            resolved=resolved,
            missing_reasons=missing_reasons,
        )

    for leg, currency in ((LEG_BASE, base), (LEG_QUOTE, quote)):
        for role in both_leg_roles:
            code, reason = resolver.resolve(role, currency)
            if code:
                resolved[(leg, role)] = code
            else:
                missing_reasons[f"{leg}:{role}"] = reason

    if non_usd_roles:
        non_usd_currency = non_usd_leg(base, quote)
        if non_usd_currency is not None:
            leg = LEG_BASE if non_usd_currency == base else LEG_QUOTE
            for role in non_usd_roles:
                code, reason = resolver.resolve(role, non_usd_currency)
                if code:
                    resolved[(leg, role)] = code
                else:
                    missing_reasons[f"{leg}:{role}"] = reason

    return PairAvailability(
        series_code=series_code,
        variant=variant,
        base_currency=base,
        quote_currency=quote,
        resolved=resolved,
        missing_reasons=missing_reasons,
    )


def _report_role_columns(spec: AvailabilitySpec) -> List[str]:
    """`{leg}_{role}` columns for every role spec.required_roles references, both legs.

    Derived from spec.role_filters/required_roles (not hardcoded) so the
    report's schema can never drift from what resolution actually
    produces. A column irrelevant to a given pair is simply always null
    for it, same as any variant that doesn't use a role at all.
    """
    roles = [
        role
        for role in spec.role_filters
        if any(role in both or role in non_usd for both, non_usd in spec.required_roles.values())
    ]
    return [f"{leg}_{role}" for leg in LEGS for role in roles]


def build_availability_report(
    pairs_by_variant: Dict[str, pd.DataFrame],
    data_api: Any,
    spec: AvailabilitySpec,
) -> pd.DataFrame:
    """Build the full data_availability report: one row per pair across every variant.

    pairs_by_variant maps variant name -> that variant's metadata frame
    (e.g. FXDevelopedMarkets().info).

    Builds exactly ONE RoleResolver (one get_metadata() call) and shares it
    across every pair/variant in this call -- role/currency combos repeat
    heavily (every pair has a USD leg), so resolving all of them from one
    in-memory snapshot instead of a fresh query per (role, currency) is
    what keeps this to a single database round trip regardless of pair
    count. Every resolved (leg, role) is persisted as a flat
    `{leg}_{role}` column (see _report_role_columns) so a downstream
    consumer can reconstruct each pair's PairAvailability via
    PairAvailability.from_report_row without re-resolving anything.
    """
    resolver = RoleResolver.from_data_api(data_api, spec)
    role_columns = _report_role_columns(spec)
    rows = []
    for variant, metadata in pairs_by_variant.items():
        for series_code in metadata["series_code"]:
            availability = assess_pair_availability(series_code, variant, resolver, spec)
            row = {
                "series_code": availability.series_code,
                "variant": availability.variant,
                "base_currency": availability.base_currency,
                "quote_currency": availability.quote_currency,
                "blocked": availability.blocked,
                "block_reasons": "; ".join(availability.block_reasons),
            }
            for leg in LEGS:
                for role in spec.role_filters:
                    row[f"{leg}_{role}"] = availability.get(leg, role)
            rows.append(row)
    return pd.DataFrame(
        rows,
        columns=(
            [
                "series_code",
                "variant",
                "base_currency",
                "quote_currency",
                "blocked",
                "block_reasons",
            ]
            + role_columns
        ),
    )


def pairs_from_availability_report(
    report: pd.DataFrame, spec: AvailabilitySpec
) -> List[PairAvailability]:
    """Every PairAvailability in `report` -- one variant's build_availability_report output.

    Reconstructs each row via PairAvailability.from_report_row -- no
    data_api, no metadata query.
    """
    if report.empty:
        return []
    return [PairAvailability.from_report_row(row, spec) for _, row in report.iterrows()]
