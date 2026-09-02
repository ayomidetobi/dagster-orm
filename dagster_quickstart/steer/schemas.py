"""Pandera schemas for the three STEER table boundaries: features, estimates, signals.

Mirrors assets/ingestion/bloomberg_rewrite/check.py's pattern -- a
pa.DataFrameSchema per table, validated with lazy=True so every failure is
collected in one pass, wired into the asset via an AssetCheckSpec so a bad
partition fails loudly instead of reaching the regression.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandera as pa

from dagster_quickstart.steer.config import DRIVER_NAMES
from dagster_quickstart.steer.features import (
    IS_LOGGED_COLUMN,
    RATE_COLUMN,
    REALIZED_VOLATILITY_COLUMN,
)

#: Plausible bounds for an FX spot rate -- wide enough to cover every G10/EM
#: pair (from ~0.001 JPY-style quote conventions up to ~2000 for some EM
#: pairs quoted in local-currency-per-USD) without being a no-op check.
_RATE_MIN, _RATE_MAX = 1e-4, 1e5

#: A coefficient/z-score/fitted-value this large means something upstream
#: broke (bad units, a divide-by-near-zero) -- not a real market regime.
_SANITY_BOUND = 1e6


def _finite(series) -> bool:
    return bool(np.isfinite(series.dropna()).all())


def steer_features_schema(drivers: Sequence[str] = DRIVER_NAMES) -> pa.DataFrameSchema:
    """Pandera schema for steer_features, for a specific universe's driver set (5 for G10/EM, 7 for CHN).

    A module-level constant can't do this -- CHN's steer_features has 2
    columns (offshore_spread, flows) a schema built from the fixed 5
    DRIVER_NAMES would never validate (or would silently ignore, since
    strict=False). Build one from StrategyConfig.drivers per universe
    instead of importing a single shared schema.
    """
    return pa.DataFrameSchema(
        columns={
            RATE_COLUMN: pa.Column(
                float, nullable=False, checks=[pa.Check.in_range(_RATE_MIN, _RATE_MAX)]
            ),
            **{
                driver: pa.Column(
                    float,
                    nullable=True,
                    checks=[pa.Check(_finite, error=f"{driver} contains a non-finite value")],
                )
                for driver in drivers
            },
            REALIZED_VOLATILITY_COLUMN: pa.Column(
                float, nullable=True, checks=[pa.Check.ge(0), pa.Check.le(1.0)]
            ),
            IS_LOGGED_COLUMN: pa.Column(bool, nullable=False),
        },
        index=pa.Index(pa.DateTime, unique=True),
        coerce=True,
        strict=False,
    )


def steer_estimates_schema(drivers: Sequence[str] = DRIVER_NAMES) -> pa.DataFrameSchema:
    """Pandera schema for gold.steer_estimates, for a specific universe's driver set -- see steer_features_schema."""
    return pa.DataFrameSchema(
        columns={
            "date": pa.Column(pa.DateTime, nullable=False),
            "universe": pa.Column(str, nullable=False, checks=pa.Check.isin(["G10", "EM", "CHN"])),
            "series_code": pa.Column(str, nullable=False),
            "is_logged": pa.Column(bool, nullable=False),
            "const_coef": pa.Column(
                float, nullable=True, checks=pa.Check.in_range(-_SANITY_BOUND, _SANITY_BOUND)
            ),
            **{
                f"{driver}_coef": pa.Column(
                    float, nullable=True, checks=pa.Check.in_range(-_SANITY_BOUND, _SANITY_BOUND)
                )
                for driver in drivers
            },
            "fitted_value": pa.Column(float, nullable=False),
            "actual_value": pa.Column(float, nullable=False),
            "z_score": pa.Column(float, nullable=False, checks=pa.Check.in_range(-100, 100)),
            "r_squared": pa.Column(float, nullable=False, checks=pa.Check.in_range(0.0, 1.0001)),
            "n_obs": pa.Column(int, nullable=False, checks=pa.Check.gt(0)),
            "cointegration_passed": pa.Column(bool, nullable=False),
            "sign_dropped": pa.Column(bool, nullable=False),
            "dropped_variables": pa.Column(str, nullable=True),
        },
        coerce=True,
        strict=False,
    )


STEER_SIGNALS_SCHEMA = pa.DataFrameSchema(
    columns={
        "date": pa.Column(pa.DateTime, nullable=False),
        "universe": pa.Column(str, nullable=False, checks=pa.Check.isin(["G10", "EM", "CHN"])),
        "series_code": pa.Column(str, nullable=False),
        "signal": pa.Column(str, nullable=False, checks=pa.Check.isin(["BUY", "SELL", "NONE"])),
        "entry_z_score": pa.Column(float, nullable=False, checks=pa.Check.in_range(-100, 100)),
        "target": pa.Column(float, nullable=True, checks=pa.Check.in_range(_RATE_MIN, _RATE_MAX)),
        "stop_loss": pa.Column(
            float, nullable=True, checks=pa.Check.in_range(_RATE_MIN, _RATE_MAX)
        ),
        "reason": pa.Column(str, nullable=False),
    },
    coerce=True,
    strict=False,
)
