"""Dagster resource exposing every variant's FXVariant (steer/config.py).

FX_G10/FX_EM/FX_CHN are already validated code-defined singletons (see steer/config.py's
module docstring) -- this resource no longer loads or validates anything itself, it just gives
assets a stable `context.resources.steer_config.for_variant(...)` entry point so they don't
reach into steer.config directly.
"""

from __future__ import annotations

from dagster import ConfigurableResource, get_dagster_logger

from dagster_quickstart.steer.config import FXVariant, VARIANTS

logger = get_dagster_logger()


class SteerConfigResource(ConfigurableResource):
    """Dagster resource wrapping steer.config.VARIANTS.

    Use context.resources.steer_config.for_variant("G10"|"EM"|"CHN") inside runs.
    """

    def for_variant(self, variant: str) -> FXVariant:
        if variant not in VARIANTS:
            raise KeyError(f"No FXVariant for variant {variant!r} -- have {sorted(VARIANTS)}")
        return VARIANTS[variant]
