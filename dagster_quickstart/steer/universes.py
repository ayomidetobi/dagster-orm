"""FX_G10/FX_EM/FX_CHN: code-defined STEER universes, config and entry point in one object.

Replaces loading a universe's StrategyConfig from strategy_configs/*.yaml + wiring a Steer by
hand:

    strategy_config = load_strategy_config("strategy_configs/g10.yaml")
    steer = Steer.from_data_api(data_api, universe="G10", strategy_config=strategy_config)
    results = steer.fit(lookback_days=5, cointegration="each")

with:

    from dagster_quickstart.steer.universes import FX_G10
    results = FX_G10.fit(lookback_days=5, cointegration="each")

FXUniverse subclasses StrategyConfig (steer/config.py) rather than replacing it -- every
field, and the expected_signs-covers-drivers pydantic validation, is exactly StrategyConfig's;
this module only adds .steer()/.fit() and the 3 module-level singletons + the UNIVERSES
lookup dict, and freezes the instances (see FXUniverse's docstring for why).

No Dagster import here (see tests/test_steer_library_boundary.py) -- same boundary as the
rest of steer/. DataAPI (rewrite.data_api, not Dagster) is also never constructed at import
time: `import dagster_quickstart.steer.universes` must not open a Postgres/S3 connection --
default_data_api() is only ever called lazily, from inside .steer()/.fit(), and only when the
caller didn't pass its own data_api (see tests/test_steer_universes.py's
test_importing_universes_constructs_no_data_api).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from dagster_quickstart.steer.config import StrategyConfig
from dagster_quickstart.steer.constants import (
    DRIVER_COMMODITY,
    DRIVER_FLOWS,
    DRIVER_GLOBAL_EQUITY,
    DRIVER_INTEREST_RATE_DIFFERENTIAL,
    DRIVER_LOCAL_EQUITY,
    DRIVER_NAMES,
    DRIVER_OFFSHORE_SPREAD,
    DRIVER_YIELD_CURVE_OR_CDS,
    UNIVERSE_CHN,
    UNIVERSE_EM,
    UNIVERSE_G10,
)

if TYPE_CHECKING:
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI
    from dagster_quickstart.steer.model import Steer, SteerResults


def default_data_api() -> "DataAPI":
    """The DataAPI .steer()/.fit() build when the caller doesn't pass one of its own.

    Imported inside this function, not at module level -- constructing a DataAPI attaches
    DuckLake (Postgres + S3), and merely `import`ing this module (e.g. in a test suite, or
    anywhere that just wants FX_G10.window_months) must never do that. live=False matches
    every other zero-config DataAPI call site in this repo (see scripts/example_dataapi.py,
    scripts/example_steer.py).
    """
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

    return DataAPI(live=False)


class FXUniverse(StrategyConfig):
    """A STEER universe's parameters, with the pipeline attached.

    Config and entry point in one object: FX_G10.fit(...) instead of loading YAML and wiring
    a Steer by hand. Adds no fields over StrategyConfig -- just .steer()/.fit() -- so every
    validation StrategyConfig already does (expected_signs covering exactly `drivers`, the
    universe/expected_signs field constraints, etc.) applies unchanged.

    frozen=True matters: FX_G10/FX_EM/FX_CHN below are module-level singletons shared by
    every caller in the process -- without it, `FX_G10.z_threshold = 2.0` in one script would
    silently change every other caller's behavior too (assigning to a frozen field instead
    raises pydantic.ValidationError). To experiment with a variant, make an independent copy
    instead of mutating the shared instance:

        custom_g10 = FX_G10.model_copy(update={"z_threshold": 2.0})

    model_copy() returns a new FXUniverse -- FX_G10 itself, and every other caller holding it,
    is untouched.
    """

    model_config = {"extra": "forbid", "frozen": True}

    def steer(self, data_api: Optional[Any] = None) -> "Steer":
        """A Steer wired to this universe's config, over `data_api` (default_data_api() if omitted)."""
        from dagster_quickstart.steer.model import Steer

        return Steer.from_data_api(
            data_api if data_api is not None else default_data_api(),
            universe=self.universe,
            strategy_config=self,
        )

    def fit(self, *, data_api: Optional[Any] = None, **kwargs: Any) -> "SteerResults":
        """Fit every pair in this universe. See Steer.fit for the keyword arguments.

        data_api defaults to a fresh default_data_api() -- pass one explicitly (a fake/stub in
        tests, or a DataAPI already wired to a specific run's cache) to override it without
        touching that default.
        """
        return self.steer(data_api).fit(**kwargs)


#: Transcribed verbatim from the fields that used to live in
#: strategy_configs/g10.yaml/em.yaml/chn.yaml (see git history for the deleted files) --
#: window_months, stop_reward_ratio, logged_rate_threshold, cointegration_significance,
#: min_observations, and expected_signs all differ per universe; ticker_source and
#: logged_rate_vol_window_days happen to be identical across all 3 but were explicit fields
#: in every YAML file, so they're explicit here too.
FX_G10 = FXUniverse(
    universe=UNIVERSE_G10,
    ticker_source="bloomberg",
    window_months=12,
    z_threshold=1.5,
    stop_reward_ratio=2.0,
    logged_rate_threshold=0.01,
    logged_rate_vol_window_days=20,
    cointegration_significance=0.05,
    min_observations=60,
    expected_signs={
        DRIVER_INTEREST_RATE_DIFFERENTIAL: 1,
        DRIVER_YIELD_CURVE_OR_CDS: -1,
        DRIVER_LOCAL_EQUITY: 1,
        DRIVER_GLOBAL_EQUITY: 1,
        DRIVER_COMMODITY: 1,
    },
)

FX_EM = FXUniverse(
    universe=UNIVERSE_EM,
    ticker_source="bloomberg",
    window_months=6,
    z_threshold=1.5,
    stop_reward_ratio=1.0,
    logged_rate_threshold=0.0025,
    logged_rate_vol_window_days=20,
    cointegration_significance=0.05,
    min_observations=60,
    expected_signs={
        DRIVER_INTEREST_RATE_DIFFERENTIAL: 1,
        DRIVER_YIELD_CURVE_OR_CDS: -1,
        DRIVER_LOCAL_EQUITY: 1,
        DRIVER_GLOBAL_EQUITY: 1,
        DRIVER_COMMODITY: 1,
    },
)

FX_CHN = FXUniverse(
    universe=UNIVERSE_CHN,
    ticker_source="bloomberg",
    window_months=6,
    z_threshold=1.5,
    stop_reward_ratio=1.0,
    logged_rate_threshold=0.0025,
    logged_rate_vol_window_days=20,
    cointegration_significance=0.01,
    min_observations=60,
    drivers=DRIVER_NAMES + (DRIVER_OFFSHORE_SPREAD, DRIVER_FLOWS),
    expected_signs={
        DRIVER_INTEREST_RATE_DIFFERENTIAL: 1,
        DRIVER_YIELD_CURVE_OR_CDS: -1,
        DRIVER_LOCAL_EQUITY: 1,
        DRIVER_GLOBAL_EQUITY: 1,
        DRIVER_COMMODITY: 1,
        # Positivity constraint explicitly removed by the USDCNH spec note -- 0 means "no
        # expectation, never drop" (see steer.estimation.sign_check_and_reestimate).
        DRIVER_OFFSHORE_SPREAD: 0,
        # Sign flips across the 2024-08-16 regime cutover (see steer/features.py's
        # build_chn_flows) -- no single fixed sign is right on both sides.
        DRIVER_FLOWS: 0,
    },
)

#: universe name -> its FXUniverse -- for anything that resolves a universe by string (e.g. a
#: CLI --universe flag; see scripts/example_steer.py).
UNIVERSES: Dict[str, FXUniverse] = {u.universe: u for u in (FX_G10, FX_EM, FX_CHN)}
