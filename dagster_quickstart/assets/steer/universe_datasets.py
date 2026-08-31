"""Universe -> FX dataset class mapping, and the pair-discovery helper shared by every STEER asset.

Every currency_pair (series_code) in a universe is fetched here, fresh,
each time an asset needs it -- there's no caching across runs and no
partition per pair (see assets/steer/partitions.py's module docstring for
why: G10/EM/CHN are the only Dagster partitions; currency_pair is data a
partition's run discovers and loops over, not a Dagster partition itself).
"""

from typing import Dict, Type

import pandas as pd

from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase
from dagster_quickstart.rewrite.data_api.dataset.fx import (
    FXChina,
    FXDevelopedMarkets,
    FXEmergingMarkets,
)

UNIVERSE_TO_DATASET_CLASS: Dict[str, Type[DatasetBase]] = {
    "G10": FXDevelopedMarkets,
    "EM": FXEmergingMarkets,
    "CHN": FXChina,
}


def discover_pairs(universe: str, data_api) -> pd.DataFrame:
    """Metadata for every real currency_pair (series_code) in `universe`."""
    DatasetBase.configure(data_api)
    dataset_cls = UNIVERSE_TO_DATASET_CLASS[universe]
    return dataset_cls().info
