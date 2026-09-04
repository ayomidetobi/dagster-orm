"""AvailabilitySpec: the SHAPE of an availability check, with no domain-specific values.

A caller (e.g. STEER_AVAILABILITY_SPEC) builds one instance carrying its own role/variant
vocabulary; every function in this package that needs to know which roles matter takes that
instance as an explicit argument rather than assuming any particular one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class AvailabilitySpec:
    """role_filters/required_roles/single_non_usd_leg are keyed by whatever role/variant
    string vocabulary the caller chooses -- this package never inspects those strings, only
    matches metadata rows against role_filters and looks up required_roles/single_non_usd_leg
    by the `variant` value passed to assess_pair_availability().

    role_filters: role name -> the metadata filter (column -> allowed values) that resolves
        it, e.g. {"swap_2y": {"sub_asset_class": ["Interest Rate Swap"], "tenor": ["2Y"]}}.

    required_roles: variant -> (roles required for BOTH legs, roles required for the non-USD
        leg only). Unchanged shape from the pre-extraction REQUIRED_ROLES -- this part was
        already a straight lift.

    single_non_usd_leg: variant -> whether that variant requires the pair to have exactly one
        non-USD leg (and is blocked outright if it doesn't, before any role resolution is
        attempted). Kept separate from required_roles rather than inferred from
        bool(non_usd_roles) -- the invariant and the roles are separate facts: a variant could
        need non_usd_roles without requiring exactly one non-USD leg, or vice versa.

    single_non_usd_leg_reason: the block reason used when single_non_usd_leg[variant] is True
        and the pair doesn't satisfy it. One spec-wide string (not per-variant) since every
        variant that sets single_non_usd_leg uses identical wording today; a spec with
        genuinely different per-variant wording would need this promoted to a dict too.

    excluded_series_codes: role -> series_codes that must never resolve for it, checked before
        role_filters' matches are grouped by currency (see RoleResolver). Exists for a
        catalog row that would otherwise match a role's filters and create a genuine ambiguity
        (RoleResolver.ambiguities) that the catalog itself doesn't yet have a column to resolve
        (e.g. two "vintages" of the same underlying series, live vs. close) -- an explicit,
        named exclusion instead of a silent tie-break on sort order or an incidental
        data-quality-flag column. Empty for a role with no such exclusion.

    variants: every variant this spec covers -- e.g. for deriving Dagster partition keys
        without importing anything specific to whichever domain built this spec.
    """

    role_filters: Dict[str, Dict[str, List[str]]]
    required_roles: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]]
    single_non_usd_leg: Dict[str, bool]
    single_non_usd_leg_reason: str
    excluded_series_codes: Dict[str, Tuple[str, ...]]
    variants: Tuple[str, ...]
