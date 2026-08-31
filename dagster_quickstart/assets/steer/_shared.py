"""Shared helpers for the STEER asset graph.

Not prefixed with an underscore out of secrecy -- just signals "asset-graph
plumbing, not part of the steer/ business-logic package" the way
assets/utils/ did before it was removed with the legacy pipeline it served.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

from dagster_quickstart.steer.discovery import PairAvailability, assess_pair_availability


@dataclass(frozen=True)
class SteerPair:
    """One currency_pair (series_code) within a universe, plus its driver-availability assessment."""

    series_code: str
    availability: PairAvailability


def resolve_universe_pairs(
    universe: str,
    data_api: Any,
    *,
    currency_to_fi_series: Dict[str, List[str]],
    currency_to_equity_series: Dict[str, List[str]],
) -> List[SteerPair]:
    """Every real pair in `universe` (see universe_datasets.discover_pairs), each with its availability assessment."""
    from dagster_quickstart.assets.steer.universe_datasets import discover_pairs

    metadata = discover_pairs(universe, data_api)
    if metadata.empty:
        return []

    return [
        SteerPair(
            series_code=series_code,
            availability=assess_pair_availability(
                series_code,
                universe,
                currency_to_fi_series=currency_to_fi_series,
                currency_to_equity_series=currency_to_equity_series,
            ),
        )
        for series_code in metadata["series_code"]
    ]
