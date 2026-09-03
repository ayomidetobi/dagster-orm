"""Dagster resource exposing every universe's FXUniverse (steer/config.py).

FX_G10/FX_EM/FX_CHN are already validated code-defined singletons (see steer/config.py's
module docstring) -- this resource no longer loads or validates anything itself, it just gives
assets a stable `context.resources.steer_config.for_universe(...)` entry point so they don't
reach into steer.config directly.
"""

from __future__ import annotations

from dagster import ConfigurableResource, get_dagster_logger

from dagster_quickstart.steer.config import FXUniverse, UNIVERSES

logger = get_dagster_logger()


class SteerConfigResource(ConfigurableResource):
    """Dagster resource wrapping steer.config.UNIVERSES.

    Use context.resources.steer_config.for_universe("G10"|"EM"|"CHN") inside runs.
    """

    def for_universe(self, universe: str) -> FXUniverse:
        if universe not in UNIVERSES:
            raise KeyError(f"No FXUniverse for universe {universe!r} -- have {sorted(UNIVERSES)}")
        return UNIVERSES[universe]
