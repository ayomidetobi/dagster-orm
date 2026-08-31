"""Dagster resource loading/validating every universe's StrategyConfig at job start.

Loaded once in setup_for_execution() (process start, not per-asset-call) so
a bad YAML file fails the whole run immediately with a clear pydantic
error, rather than partway through a partition.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger

from dagster_quickstart.steer.config import DEFAULT_CONFIG_DIR

if TYPE_CHECKING:
    from dagster_quickstart.steer.config import StrategyConfig

logger = get_dagster_logger()


class SteerConfigResource(ConfigurableResource):
    """Dagster resource wrapping steer.config.load_all_strategy_configs().

    Use context.resources.steer_config.for_universe("G10"|"EM") inside runs.
    """

    config_dir: str = str(DEFAULT_CONFIG_DIR)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from dagster_quickstart.steer.config import load_all_strategy_configs

        self._configs = load_all_strategy_configs(self.config_dir)
        logger.info("SteerConfigResource ready (universes=%s)", sorted(self._configs))

    def for_universe(self, universe: str) -> "StrategyConfig":
        configs: Optional[Dict[str, "StrategyConfig"]] = getattr(self, "_configs", None)
        if configs is None:
            raise RuntimeError(
                "SteerConfigResource is not initialized; use only inside a "
                "Dagster run (context.resources.steer_config)."
            )
        if universe not in configs:
            raise KeyError(
                f"No StrategyConfig loaded for universe {universe!r} -- have {sorted(configs)}"
            )
        return configs[universe]
