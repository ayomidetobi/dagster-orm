"""Shared helpers for the STEER asset graph.

Not prefixed with an underscore out of secrecy -- just signals "asset-graph
plumbing, not part of the steer/ business-logic package" the way
assets/utils/ did before it was removed with the legacy pipeline it served.
"""

from dataclasses import dataclass
from typing import List

import pandas as pd

from dagster_quickstart.steer.discovery import PairAvailability


@dataclass(frozen=True)
class SteerPair:
    """One currency_pair (series_code) within a universe, plus its driver-availability assessment."""

    series_code: str
    availability: PairAvailability


def pairs_from_availability_report(report: pd.DataFrame) -> List[SteerPair]:
    """Every SteerPair in `report` -- one universe's steer_data_availability output.

    Reconstructs each row's PairAvailability via PairAvailability.from_report_row
    -- no data_api, no metadata query. Replaces the old resolve_universe_pairs,
    which re-resolved every pair's roles from scratch even though
    steer_data_availability had just done exactly that work for the same
    partition; steer_silver_prices now depends on that asset directly (see
    silver_asset.py) and calls this on its output instead.
    """
    if report.empty:
        return []
    return [
        SteerPair(
            series_code=str(row["series_code"]),
            availability=PairAvailability.from_report_row(row),
        )
        for _, row in report.iterrows()
    ]
