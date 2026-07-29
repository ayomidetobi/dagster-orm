"""Load meta series asset module.

validate_metadata_quality now runs as an in-asset check (see asset.py's
check_specs) rather than a standalone @asset_check -- it's part of
load_meta_series_to_s3's own AssetsDefinition, not a separately importable
object.
"""

from dagster_quickstart.assets.load_metaseries.asset import load_meta_series_to_s3

__all__ = [
    "load_meta_series_to_s3",
]
