"""RoleResolver: (role, currency) -> series_code, from one in-memory metadata snapshot.

Fetches the whole metadata table *once* (get_metadata() with no filters) and indexes it in
memory, rather than issuing one get_metadata() call per (role, currency) pair -- with STEER's
~195 catalog rows and ~106 distinct (role, currency) combinations across every variant, that
was ~106 real database round trips per run for a table small enough to just hold in memory. A
role is looked up by currency alone (no variant/market_development filter): a role's own
currency column already picks it out uniquely per the caller's role_filters (see
AvailabilitySpec), and a currency can be the anchor leg of pairs in more than one variant (e.g.
USD), so filtering by variant would silently fail to find rows that are genuinely shared.

All catalog data is treated as real for resolution purposes -- there is no data-quality-flag
filter or sort key here, only series_code order. A role matching more than one row for the same
currency (after spec.excluded_series_codes) is a genuine catalog ambiguity, not something
silently resolved by sort order: it's recorded in `ambiguities` and logged immediately, and the
actual pick (the alphabetically-first remaining series_code) is exactly that -- a deterministic
pick, not a principled one. A spec that needs a specific series_code excluded from a role (e.g.
a placeholder ticker that would otherwise create exactly this ambiguity) does so explicitly via
spec.excluded_series_codes, not via an incidental catalog column.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd
import structlog

from dagster_quickstart.availability.spec import AvailabilitySpec

logger = structlog.get_logger(__name__)


class RoleResolver:
    """Resolves (role, currency) -> series_code from one in-memory metadata snapshot, per `spec`.

    ambiguities maps (role, currency) -> every candidate series_code that matched, for any
    (role, currency) with more than one match remaining after spec.excluded_series_codes --
    populated and logged once, at construction time, not rediscovered per .resolve() call.
    """

    def __init__(self, metadata: pd.DataFrame, spec: AvailabilitySpec) -> None:
        self._index: Dict[Tuple[str, str], pd.DataFrame] = {}
        self.ambiguities: Dict[Tuple[str, str], Tuple[str, ...]] = {}

        for role, filters in spec.role_filters.items():
            if not set(filters) <= set(metadata.columns):
                continue  # this role's filter columns aren't in the frame -- no matches
            mask = pd.Series(True, index=metadata.index)
            for column, allowed in filters.items():
                mask &= metadata[column].isin(allowed)
            subset = metadata.loc[mask]

            excluded = spec.excluded_series_codes.get(role, ())
            if excluded:
                subset = subset[~subset["series_code"].isin(excluded)]
            if subset.empty:
                continue

            subset = subset.sort_values("series_code")
            for currency, group in subset.groupby("currency"):
                currency = str(currency)
                if len(group) > 1:
                    candidates = tuple(group["series_code"])
                    self.ambiguities[(role, currency)] = candidates
                    logger.warning(
                        "availability_role_ambiguous",
                        role=role,
                        currency=currency,
                        candidates=candidates,
                        resolved_to=candidates[0],
                    )
                self._index[(role, currency)] = group

    @classmethod
    def from_data_api(cls, data_api: Any, spec: AvailabilitySpec) -> "RoleResolver":
        """Build a resolver from one unfiltered get_metadata() call -- the whole catalog.

        get_metadata() with no arguments is valid (filters defaults to None,
        which resolves to no WHERE clause -- the full table).
        """
        return cls(data_api.get_metadata().frame, spec)

    def resolve(self, role: str, currency: str) -> Tuple[Optional[str], str]:
        """Resolve one (role, currency) to a series_code -- an in-memory lookup, no query.

        Returns (series_code, reason) -- series_code is None (with a "why
        not" reason) if nothing matches. If (role, currency) is ambiguous
        (see `ambiguities`), the returned series_code is the first one by
        series_code order -- a deterministic pick, already logged as
        ambiguous at construction time, not silently arbitrary.
        """
        group = self._index.get((role, currency))
        if group is None or group.empty:
            return None, f"No {role} series for {currency}."
        series_code = str(group.iloc[0]["series_code"])
        return series_code, f"{role} resolved to {series_code} for {currency}."
