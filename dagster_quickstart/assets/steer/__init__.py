"""STEER daily model pipeline: silver conform -> gold features -> cointegration -> estimate -> signal.

See dagster_quickstart/steer/ for the pure business logic these assets
wire into Dagster, and this package's README section in the repo root
README.md for how to run/backfill locally.
"""

from dagster_quickstart.assets.steer.availability_asset import steer_data_availability
from dagster_quickstart.assets.steer.cointegration_asset import steer_cointegration
from dagster_quickstart.assets.steer.estimate_asset import steer_estimate
from dagster_quickstart.assets.steer.gold_features_asset import steer_features
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS
from dagster_quickstart.assets.steer.signal_asset import steer_signal
from dagster_quickstart.assets.steer.silver_asset import steer_silver_prices

steer_assets = [
    steer_data_availability,
    steer_silver_prices,
    steer_features,
    steer_cointegration,
    steer_estimate,
    steer_signal,
]

__all__ = [
    "STEER_PARTITIONS",
    "steer_assets",
    "steer_silver_prices",
    "steer_features",
    "steer_cointegration",
    "steer_estimate",
    "steer_signal",
    "steer_data_availability",
]
