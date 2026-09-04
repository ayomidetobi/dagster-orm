"""STEER-style FX fair-value model layer -- public surface.

Pure business logic (config, feature engineering, OLS/cointegration
estimation, signal generation, gold-layer storage) with no Dagster
dependency -- see dagster_quickstart/assets/steer/ for the thin Dagster
asset wiring on top, mirroring how rewrite/data_api/ holds the DuckLake
business logic that assets/ingestion/ wires into assets.

    from dagster_quickstart.steer import FX_G10
    results = FX_G10.fit(lookback_days=5, cointegration="each")

Layout: constants.py/errors.py (leaves) -> source/ (data in -- may touch DataAPI) ->
analytics/ (math out -- no I/O at all) -> config.py/orm.py/model.py (StrategyConfig/FXVariant,
DuckLake persistence, the Steer/SteerResults facade) -> run.py (the `python -m
dagster_quickstart.steer` CLI). assets/steer/ sits on top of all of it, unchanged by this
layering -- it only ever imports from here, never the reverse.
"""

from __future__ import annotations

from dagster_quickstart.steer.analytics.results import PairResult
from dagster_quickstart.steer.config import FX_CHN, FX_EM, FX_G10, VARIANTS, StrategyConfig
from dagster_quickstart.steer.model import Steer, SteerResults

__all__ = [
    "FX_G10",
    "FX_EM",
    "FX_CHN",
    "VARIANTS",
    "StrategyConfig",
    "Steer",
    "SteerResults",
    "PairResult",
]
