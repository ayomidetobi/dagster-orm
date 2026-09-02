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


def discover_pairs(universe: str, data_api, *, require_real: bool = False) -> pd.DataFrame:
    """Metadata for every real currency_pair (series_code) in `universe`.

    require_real=True excludes any pair row with is_synthetic=True. All 67
    FX pair rows are is_synthetic=False (they're real, standard-convention
    tickers -- see rewrite/data_api/dataset/fx.py), so this should never
    actually narrow the pair list; it exists so a production caller that
    filters *everything* by is_synthetic=False (the normal way to exclude
    the 24 placeholder driver rows) doesn't also silently lose every pair.
    """
    DatasetBase.configure(data_api)
    dataset_cls = UNIVERSE_TO_DATASET_CLASS[universe]
    metadata = dataset_cls().info
    if require_real and not metadata.empty and "is_synthetic" in metadata.columns:
        metadata = metadata[metadata["is_synthetic"] == False]  # noqa: E712
    return metadata
