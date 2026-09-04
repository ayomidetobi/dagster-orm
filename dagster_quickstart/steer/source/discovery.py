"""discover_pairs: which pairs does a variant contain -- against the real BNP STEER metadata catalog.

Currency pairs come from the datalake via rewrite.data_api.dataset.fx
(FXDevelopedMarkets/FXEmergingMarkets/FXChina) -- see steer/config.py.
Every pair is one non-USD currency vs USD (the catalog has no FX-spot rows
of its own; see fx.py's _STEER_FX_FILTERS docstring for why and how those
rows were added).

Driver-role availability resolution (RoleResolver, PairAvailability,
assess_pair_availability, build_availability_report, parse_fx_legs) has
moved to dagster_quickstart.availability -- that package defines the
generic (role, currency) -> series_code resolution SHAPE; this module (and
steer/config.py's STEER_AVAILABILITY_SPEC) supplies STEER's actual role
filters and required-roles-per-variant. discover_pairs() stays here rather
than moving too -- "which pairs does a variant contain" needs the
STEER-specific FXDevelopedMarkets/FXEmergingMarkets/FXChina dataset
classes, which availability/ has no reason to know about.
"""

from __future__ import annotations

from typing import Any, Dict, Type

import pandas as pd

from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase
from dagster_quickstart.rewrite.data_api.dataset.fx import (
    FXChina,
    FXDevelopedMarkets,
    FXEmergingMarkets,
)
from dagster_quickstart.steer.constants import VARIANT_CHN, VARIANT_EM, VARIANT_G10

VARIANT_TO_DATASET_CLASS: Dict[str, Type[DatasetBase]] = {
    VARIANT_G10: FXDevelopedMarkets,
    VARIANT_EM: FXEmergingMarkets,
    VARIANT_CHN: FXChina,
}


def discover_pairs(variant: str, data_api: Any, *, require_real: bool = False) -> pd.DataFrame:
    """Metadata for every real currency_pair (series_code) in `variant`.

    require_real=True excludes any pair row with is_synthetic=True. All 67
    FX pair rows are is_synthetic=False (they're real, standard-convention
    tickers -- see rewrite/data_api/dataset/fx.py), so this should never
    actually narrow the pair list; it exists so a production caller that
    filters *everything* by is_synthetic=False (the normal way to exclude
    the 24 placeholder driver rows) doesn't also silently lose every pair.
    """
    DatasetBase.configure(data_api)
    dataset_cls = VARIANT_TO_DATASET_CLASS[variant]
    metadata = dataset_cls().info
    if require_real and not metadata.empty and "is_synthetic" in metadata.columns:
        metadata = metadata[metadata["is_synthetic"] == False]  # noqa: E712
    return metadata
