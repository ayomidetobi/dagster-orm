"""assets/availability_asset.py's FX_AVAILABILITY_PARTITIONS must match STEER_PARTITIONS' keys.

fx_data_availability derives its own partitions from STEER_AVAILABILITY_SPEC.variants rather
than importing assets/steer/partitions.py's STEER_PARTITIONS directly (that would leak the
package boundary this asset's move out of assets/steer/ is meant to establish -- see
availability_asset.py's module docstring). steer_silver_prices reads the stored report keyed by
variant (dagster_quickstart.availability.storage) -- if the two partition sets ever diverged,
that read would silently return nothing for whichever variant is missing. Asserted here, not
assumed.
"""

from __future__ import annotations

from dagster_quickstart.assets.availability_asset import FX_AVAILABILITY_PARTITIONS
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS


def test_fx_availability_partitions_match_steer_partitions():
    assert set(FX_AVAILABILITY_PARTITIONS.get_partition_keys()) == set(
        STEER_PARTITIONS.get_partition_keys()
    )
