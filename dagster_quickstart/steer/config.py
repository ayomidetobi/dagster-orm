"""StrategyConfig: per-universe (G10/EM/CHN) STEER model parameters.

One StrategyConfig per universe -- no "if universe == 'EM'" branching should
ever appear in asset code; every universe-specific number (window length,
z threshold, risk/reward ratio, logged-rate threshold, expected coefficient
signs) lives here instead. FX_G10/FX_EM/FX_CHN (steer/universes.py) are the
3 real, validated instances -- see that module for why they're defined in
code rather than loaded from YAML at job start.

Currency pairs and their rate series are NOT configured here anymore --
they come straight from the datalake via rewrite.data_api.dataset.fx
(FXDevelopedMarkets/FXEmergingMarkets/FXChina), fetched at *run time* inside
each universe's asset (see assets/steer/pairs.py) -- G10/EM/CHN are
independently-fetched static Dagster partitions (one per universe), and
each partition's run processes every currency_pair in that universe as data,
not as separate Dagster partitions (see assets/steer/partitions.py). Two
drivers are genuinely single global series applied identically to every
pair in every universe (global_equity_series, commodity_series) -- see
GLOBAL_DRIVERS below, which is why they live in code once rather than as
YAML fields copy-pasted into every strategy_configs/*.yaml (that would let
G10/EM/CHN's copies silently drift apart, contradicting "applied
identically"). local_equity and the rate-based drivers
(interest_rate_differential/yield_curve_or_cds) are discovered and
availability-checked per pair instead (see steer/discovery.py) -- a pair
missing genuine per-country data for either is blocked rather than silently
regressed on a proxy.
"""

from __future__ import annotations

from typing import Dict, Literal, Tuple

from pydantic import BaseModel, Field, model_validator

from dagster_quickstart.steer.constants import DRIVER_NAMES


class GlobalDriverConfig(BaseModel):
    """The 2 STEER drivers that are the same single series_code for every universe.

    global_equity_series/commodity_series are a curation choice (which
    benchmark to use), not a per-universe setting -- G10/EM/CHN apply the
    identical series to every pair. Defined once here (in code) rather
    than duplicated across every universe's config so the 3 universes
    can't drift apart; StrategyConfig.global_equity_series/commodity_series
    read from GLOBAL_DRIVERS below instead of being their own fields.
    """

    model_config = {"extra": "forbid", "frozen": True}

    global_equity_series: str
    commodity_series: str


#: The one shared instance every universe's StrategyConfig uses -- see
#: GlobalDriverConfig's docstring. MXWO (MSCI World) and BRENT are the
#: real global benchmarks in the STEER metadata catalog (meta_series_steer.csv).
GLOBAL_DRIVERS = GlobalDriverConfig(
    global_equity_series="MXWO_PX_LAST",
    commodity_series="BRENT_PX_LAST",
)


class StrategyConfig(BaseModel):
    """STEER model parameters for one universe (G10, EM, or CHN).

    stop_reward_ratio is the reward:risk multiple (2.0 means a 2:1
    reward:risk stop/target -- see steer.signals.generate_signal for the
    exact stop-loss formula). logged_rate_threshold is a trailing realized
    FX-rate volatility cutoff (decimal, e.g. 0.01 == 1.00%): a pair whose
    trailing `logged_rate_vol_window_days`-day mean absolute daily % move
    exceeds this is regressed in log-rate space instead of raw level (see
    steer.features.should_use_logged_rate for the exact rule).

    global_equity_series/commodity_series are NOT StrategyConfig fields -- they're
    properties reading from the single shared GLOBAL_DRIVERS instance (see
    its docstring for why), so every universe always sees the identical
    value with no possibility of a per-universe copy drifting out of sync.
    """

    model_config = {"extra": "forbid"}

    universe: Literal["G10", "EM", "CHN"]
    ticker_source: str = "bloomberg"
    window_months: int = Field(gt=0)
    z_threshold: float = Field(default=1.5, gt=0)
    stop_reward_ratio: float = Field(gt=0)
    logged_rate_threshold: float = Field(gt=0)
    logged_rate_vol_window_days: int = Field(default=20, gt=0)
    cointegration_significance: float = Field(default=0.05, gt=0, lt=1)
    min_observations: int = Field(default=40, gt=0)
    #: This universe's driver set -- defaults to the 5 canonical
    #: DRIVER_NAMES; CHN (steer/universes.py's FX_CHN) overrides it to add
    #: offshore_spread/flows on top, since estimation.py is generic over
    #: however many columns `drivers` (steer/features.py) produces.
    drivers: Tuple[str, ...] = DRIVER_NAMES
    #: Expected coefficient sign per driver (+1/-1); 0 means "no expectation,
    #: never drop this driver" -- see steer.estimation.sign_check_and_reestimate.
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
