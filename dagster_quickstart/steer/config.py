"""StrategyConfig: per-variant (G10/EM/CHN) STEER model parameters. FX_G10/FX_EM/FX_CHN:
code-defined instances of it, config and entry point in one object.

One StrategyConfig per variant -- no "if variant == 'EM'" branching should
ever appear in asset code; every variant-specific number (window length,
z threshold, risk/reward ratio, logged-rate threshold, expected coefficient
signs) lives here instead.

Currency pairs and their rate series are NOT configured here anymore --
they come straight from the datalake via rewrite.data_api.dataset.fx
(FXDevelopedMarkets/FXEmergingMarkets/FXChina), fetched at *run time* inside
each variant's asset (see assets/steer/pairs.py) -- G10/EM/CHN are
independently-fetched static Dagster partitions (one per variant), and
each partition's run processes every currency_pair in that variant as data,
not as separate Dagster partitions (see assets/steer/partitions.py). Two
drivers are genuinely single global series applied identically to every
pair in every variant (global_equity_series, commodity_series) -- see
GLOBAL_DRIVERS below, which is why they live in code once rather than as
per-variant fields (that would let G10/EM/CHN's copies silently drift
apart, contradicting "applied identically"). local_equity and the
rate-based drivers (interest_rate_differential/yield_curve_or_cds) are
discovered and availability-checked per pair instead (see
steer/source/discovery.py) -- a pair missing genuine per-country data for
either is blocked rather than silently regressed on a proxy.

FX_G10/FX_EM/FX_CHN replace loading a variant's StrategyConfig from YAML + wiring a Steer by
hand:

    strategy_config = load_strategy_config("strategy_configs/g10.yaml")
    steer = Steer.from_data_api(data_api, variant="G10", strategy_config=strategy_config)
    results = steer.fit(lookback_days=5, cointegration="each")

with:

    from dagster_quickstart.steer.config import FX_G10
    results = FX_G10.fit(lookback_days=5, cointegration="each")

FXVariant subclasses StrategyConfig rather than replacing it -- every field, and the
expected_signs-covers-drivers pydantic validation, is exactly StrategyConfig's; this module
only adds .steer()/.fit() and the 3 module-level singletons + the VARIANTS lookup dict, and
freezes the instances (see FXVariant's docstring for why).

No Dagster import here (see tests/test_steer_library_boundary.py) -- same boundary as the rest
of steer/. DataAPI (rewrite.data_api, not Dagster) is also never constructed at import time:
`import dagster_quickstart.steer.config` must not open a Postgres/S3 connection --
default_data_api() is only ever called lazily, from inside .steer()/.fit(), and only when the
caller didn't pass its own data_api (see tests/test_steer_variants.py's
test_importing_config_constructs_no_data_api).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Tuple

from pydantic import BaseModel, Field, model_validator

from dagster_quickstart.steer.constants import (
    DRIVER_COMMODITY,
    DRIVER_FLOWS,
    DRIVER_GLOBAL_EQUITY,
    DRIVER_INTEREST_RATE_DIFFERENTIAL,
    DRIVER_LOCAL_EQUITY,
    DRIVER_NAMES,
    DRIVER_OFFSHORE_SPREAD,
    DRIVER_YIELD_CURVE_OR_CDS,
    VARIANT_CHN,
    VARIANT_EM,
    VARIANT_G10,
)

if TYPE_CHECKING:
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI
    from dagster_quickstart.steer.model import Steer, SteerPanel


class GlobalDriverConfig(BaseModel):
    """The 2 STEER drivers that are the same single series_code for every variant.

    global_equity_series/commodity_series are a curation choice (which
    benchmark to use), not a per-variant setting -- G10/EM/CHN apply the
    identical series to every pair. Defined once here (in code) rather
    than duplicated across every variant's config so the 3 variants
    can't drift apart; StrategyConfig.global_equity_series/commodity_series
    read from GLOBAL_DRIVERS below instead of being their own fields.
    """

    model_config = {"extra": "forbid", "frozen": True}

    global_equity_series: str
    commodity_series: str


#: The one shared instance every variant's StrategyConfig uses -- see
#: GlobalDriverConfig's docstring. MXWO (MSCI World) and BRENT are the
#: real global benchmarks in the STEER metadata catalog (meta_series_steer.csv).
GLOBAL_DRIVERS = GlobalDriverConfig(
    global_equity_series="MXWO_PX_LAST",
    commodity_series="BRENT_PX_LAST",
)


class StrategyConfig(BaseModel):
    """STEER model parameters for one variant (G10, EM, or CHN).

    stop_reward_ratio is the reward:risk multiple (2.0 means a 2:1
    reward:risk stop/target -- see generate_signal, steer/analytics/estimation.py, for the
    exact stop-loss formula). logged_rate_threshold is a trailing realized
    FX-rate volatility cutoff (decimal, e.g. 0.01 == 1.00%): a pair whose
    trailing `logged_rate_vol_window_days`-day mean absolute daily % move
    exceeds this is regressed in log-rate space instead of raw level (see
    steer.source.features.should_use_logged_rate for the exact rule).

    global_equity_series/commodity_series are NOT StrategyConfig fields -- they're
    properties reading from the single shared GLOBAL_DRIVERS instance (see
    its docstring for why), so every variant always sees the identical
    value with no possibility of a per-variant copy drifting out of sync.
    """

    model_config = {"extra": "forbid"}

    variant: Literal["G10", "EM", "CHN"]
    ticker_source: str = "bloomberg"
    window_months: int = Field(gt=0)
    z_threshold: float = Field(default=1.5, gt=0)
    stop_reward_ratio: float = Field(gt=0)
    logged_rate_threshold: float = Field(gt=0)
    logged_rate_vol_window_days: int = Field(default=20, gt=0)
    cointegration_significance: float = Field(default=0.05, gt=0, lt=1)
    min_observations: int = Field(default=40, gt=0)
    #: This variant's driver set -- defaults to the 5 canonical
    #: DRIVER_NAMES; FX_CHN below overrides it to add
    #: offshore_spread/flows on top, since analytics/estimation.py is generic over
    #: however many columns `drivers` (steer/source/features.py) produces.
    drivers: Tuple[str, ...] = DRIVER_NAMES
    #: Expected coefficient sign per driver (+1/-1); 0 means "no expectation,
    #: never drop this driver" -- see sign_check_and_reestimate, steer/analytics/estimation.py.
    expected_signs: Dict[str, Literal[-1, 0, 1]]

    @property
    def global_equity_series(self) -> str:
        return GLOBAL_DRIVERS.global_equity_series

    @property
    def commodity_series(self) -> str:
        return GLOBAL_DRIVERS.commodity_series

    @model_validator(mode="after")
    def _expected_signs_cover_every_driver(self) -> "StrategyConfig":
        missing = set(self.drivers) - set(self.expected_signs)
        if missing:
            raise ValueError(
                f"expected_signs is missing driver(s): {sorted(missing)} -- "
                f"every one of {self.drivers} needs an expected sign (use 0 for 'no expectation')."
            )
        extra = set(self.expected_signs) - set(self.drivers)
        if extra:
            raise ValueError(
                f"expected_signs has driver(s) not in this config's drivers: {sorted(extra)}."
            )
        return self


def default_data_api() -> "DataAPI":
    """The DataAPI .steer()/.fit() build when the caller doesn't pass one of its own.

    Imported inside this function, not at module level -- constructing a DataAPI attaches
    DuckLake (Postgres + S3), and merely `import`ing this module (e.g. in a test suite, or
    anywhere that just wants FX_G10.window_months) must never do that. live=False matches
    every other zero-config DataAPI call site in this repo (see steer/run.py).
    """
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

    return DataAPI(live=False)


class FXVariant(StrategyConfig):
    """A STEER variant's parameters, with the pipeline attached.

    Config and entry point in one object: FX_G10.fit(...) instead of loading YAML and wiring
    a Steer by hand. Adds no fields over StrategyConfig -- just .steer()/.fit() -- so every
    validation StrategyConfig already does (expected_signs covering exactly `drivers`, the
    variant/expected_signs field constraints, etc.) applies unchanged.

    frozen=True matters: FX_G10/FX_EM/FX_CHN below are module-level singletons shared by
    every caller in the process -- without it, `FX_G10.z_threshold = 2.0` in one script would
    silently change every other caller's behavior too (assigning to a frozen field instead
    raises pydantic.ValidationError). To experiment with a variant, make an independent copy
    instead of mutating the shared instance:

        custom_g10 = FX_G10.model_copy(update={"z_threshold": 2.0})

    model_copy() returns a new FXVariant -- FX_G10 itself, and every other caller holding it,
    is untouched.
    """

    model_config = {"extra": "forbid", "frozen": True}

    def steer(self, data_api: Optional[Any] = None) -> "Steer":
        """A Steer wired to this variant's config, over `data_api` (default_data_api() if omitted)."""
        from dagster_quickstart.steer.model import Steer

        return Steer.from_data_api(
            data_api if data_api is not None else default_data_api(),
            variant=self.variant,
            strategy_config=self,
        )

    def fit(self, *, data_api: Optional[Any] = None, **kwargs: Any) -> "SteerPanel":
        """Fit every pair in this variant. See Steer.fit for the keyword arguments.

        data_api defaults to a fresh default_data_api() -- pass one explicitly (a fake/stub in
        tests, or a DataAPI already wired to a specific run's cache) to override it without
        touching that default.
        """
        return self.steer(data_api).fit(**kwargs)


#: Transcribed verbatim from the fields that used to live in the now-deleted
#: strategy_configs/g10.yaml/em.yaml/chn.yaml (see git history) -- window_months,
#: stop_reward_ratio, logged_rate_threshold, cointegration_significance, min_observations, and
#: expected_signs all differ per variant; ticker_source and logged_rate_vol_window_days
#: happen to be identical across all 3 but were explicit fields in every YAML file, so they're
#: explicit here too.
FX_G10 = FXVariant(
    variant=VARIANT_G10,
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

FX_EM = FXVariant(
    variant=VARIANT_EM,
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

FX_CHN = FXVariant(
    variant=VARIANT_CHN,
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
        # expectation, never drop" (see sign_check_and_reestimate, steer/analytics/estimation.py).
        DRIVER_OFFSHORE_SPREAD: 0,
        # Sign flips across the 2024-08-16 regime cutover (see steer/source/features.py's
        # build_chn_flows) -- no single fixed sign is right on both sides.
        DRIVER_FLOWS: 0,
    },
)

#: variant name -> its FXVariant -- for anything that resolves a variant by string (e.g. a
#: CLI --variant flag; see steer/run.py).
VARIANTS: Dict[str, FXVariant] = {u.variant: u for u in (FX_G10, FX_EM, FX_CHN)}
