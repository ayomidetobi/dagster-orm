"""RoleResolver: (role, currency) -> series_code, from one in-memory metadata snapshot.

Fetches the whole metadata table *once* (get_metadata() with no filters) and indexes it in
memory, rather than issuing one get_metadata() call per (role, currency) pair -- with STEER's
~195 catalog rows and ~106 distinct (role, currency) combinations across every variant, that
was ~106 real database round trips per run for a table small enough to just hold in memory. A
role is looked up by currency alone (no variant/market_development filter): a role's own
currency column already picks it out uniquely per the caller's role_filters (see
AvailabilitySpec), and a currency can be the anchor leg of pairs in more than one variant (e.g.
USD), so filtering by variant would silently fail to find rows that are genuinely shared.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

from dagster_quickstart.availability.spec import AvailabilitySpec


class RoleResolver:
    """Resolves (role, currency) -> series_code from one in-memory metadata snapshot, per `spec`."""

    def __init__(self, metadata: pd.DataFrame, spec: AvailabilitySpec) -> None:
        self._index: Dict[Tuple[str, str], pd.DataFrame] = {}
        has_synthetic = "is_synthetic" in metadata.columns
        for role, filters in spec.role_filters.items():
            if not set(filters) <= set(metadata.columns):
                continue  # this role's filter columns aren't in the frame -- no matches
            mask = pd.Series(True, index=metadata.index)
            for column, allowed in filters.items():
                mask &= metadata[column].isin(allowed)
            subset = metadata.loc[mask]
            if subset.empty:
                continue
            # Tie-break when a role matches more than one row: real before synthetic (False <
            # True), then series_code ascending.
            sort_by = ["is_synthetic", "series_code"] if has_synthetic else ["series_code"]
            subset = subset.sort_values(sort_by)
            for currency, group in subset.groupby("currency"):
                self._index[(role, str(currency))] = group

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
        not" reason) if nothing matches.
        """
        group = self._index.get((role, currency))
        if group is None or group.empty:
            return None, f"No {role} series for {currency}."
        series_code = str(group.iloc[0]["series_code"])
        return series_code, f"{role} resolved to {series_code} for {currency}."
