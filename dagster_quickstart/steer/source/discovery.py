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


def discover_pairs(variant: str, data_api: Any) -> pd.DataFrame:
    """Metadata for every currency_pair (series_code) in `variant`.

    All catalog data is treated as real for discovery/resolution purposes
    -- no data-quality-flag filter here (see dagster_quickstart.availability's
    package docstring and steer/config.py's STEER_AVAILABILITY_SPEC for
    where a genuine catalog gap, like a placeholder driver row, is instead
    handled: reported blocked, or explicitly excluded by series_code, never
    silently dropped by a blanket flag check).
    """
    DatasetBase.configure(data_api)
    dataset_cls = VARIANT_TO_DATASET_CLASS[variant]
    return dataset_cls().info
