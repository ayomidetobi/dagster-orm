"""Dagster-level tests for the STEER asset graph, with injected fake upstream data.

Uses dagster.materialize() (per the tooling constraints -- pytest + an
in-memory DuckDB fixture + Dagster's own testing utilities) rather than
mocking. rewrite_data_api is the REAL DataAPI (rewrite.data_api.factory.create_data_api)
wired to fake in-memory storage repositories -- this exercises the real
QuerySet/MetadataService/ValueService stack the FX datasets
(FXDevelopedMarkets etc.) depend on via .query(), not just get_metadata()/
get_values() in isolation. steer_catalog is the REAL SteerCatalog wrapping
an in-memory duckdb connection.

local_equity is genuinely sourceable for 14 currencies in the real catalog
now (see steer/discovery.py's EQUITY_SERIES_TO_CURRENCY), but this
fixture's METADATA deliberately doesn't include any of those explicit
per-currency equity series (nor full rate coverage for both PAIRS' legs),
so every pair here is still blocked -- exercising the per-universe loop
and the blocking/skip path, which is real, verified behavior for a fixture
this incomplete (also confirmed live against the actual catalog before
local_equity coverage was added). assess_pair_availability()'s actual
unblocking logic (both rate_data and local_equity present) is covered
directly by test_steer_discovery.py instead; the regression/signal *math*
is covered by test_steer_estimation.py/test_steer_signals.py's pure-
function tests, since a blocked pair never reaches that code in practice.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest
from dagster import DagsterInstance, materialize

from dagster_quickstart.assets.steer.availability_asset import steer_data_availability
from dagster_quickstart.assets.steer.cointegration_asset import steer_cointegration
from dagster_quickstart.assets.steer.estimate_asset import steer_estimate
from dagster_quickstart.assets.steer.gold_features_asset import steer_features
from dagster_quickstart.assets.steer.signal_asset import steer_signal
from dagster_quickstart.assets.steer.silver_asset import steer_silver_prices
from dagster_quickstart.rewrite.data_api.dataset import DatasetBase
from dagster_quickstart.rewrite.data_api.factory import create_data_api
from dagster_quickstart.steer.config import StrategyConfig
from dagster_quickstart.steer.storage import (
    GOLD_SCHEMA,
    STEER_ESTIMATES_TABLE,
    STEER_SIGNALS_TABLE,
    SteerCatalog,
)

UNIVERSE = "G10"
PAIRS = ["AUDJPY_SPOT_0004", "EURGBP_SPOT_0005"]

METADATA = pd.DataFrame(
    {
        "series_code": PAIRS
        + ["US2Y_YIELD_0021", "JP5Y_YIELD_0047", "IDX0005_INDEX", "XAU_PX_0032"],
        "asset_class": [
            "Currency",
            "Currency",
            "Fixed Income",
            "Fixed Income",
            "Equity",
            "Commodity",
        ],
        "sub_asset_class": ["Forex Spot", "Forex Spot", None, None, None, None],
        "market_development": ["G10", "G10", "G10", "G10", "GLOBAL", "GLOBAL"],
    }
)


class FakeMetadataStorage:
    """Minimal in-memory stand-in for MetadataStorageRepository."""

    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def get_columns(self) -> list[str]:
        return list(self._frame.columns)

    def _filtered(self, filters, *, exclude=False):
        frame = self._frame
        if filters:
            mask = pd.Series(True, index=frame.index)
            for field, values in filters.items():
                mask &= frame[field].isin(values)
            frame = frame[~mask] if exclude else frame[mask]
        return frame

    def get_metadata(self, filters=None, *, exclude=False, version=None, as_of=None):
        return self._filtered(filters, exclude=exclude).reset_index(drop=True)

    def get_distinct_values(self, column, *, filters=None, exclude=False):
        return sorted(self._filtered(filters, exclude=exclude)[column].dropna().unique().tolist())

    def save_metadata(self, frame, *, fresh=False):
        raise NotImplementedError

    def refresh_metadata(self):
        pass


class FakeValueStorage:
    """Minimal in-memory stand-in for ValueStorageRepository."""

    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def get_values(self, series_codes, **kwargs):
        return self._frame[self._frame["series_code"].isin(series_codes)].reset_index(drop=True)

    def get_last_values(self, series_codes, **kwargs):
        return self._frame[self._frame["series_code"].isin(series_codes)]

    def value_exists(self, series_codes, **kwargs):
        existing = set(self._frame["series_code"].unique())
        return {code: code in existing for code in series_codes}

    def save_values(self, frame):
        raise NotImplementedError

    def delete_values(self, filters):
        raise NotImplementedError

    def get_storage_path(self):
        return None


def _values_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=2)
    rows = []
    for series_code in PAIRS + [
        "US2Y_YIELD_0021",
        "JP5Y_YIELD_0047",
        "IDX0005_INDEX",
        "XAU_PX_0032",
    ]:
        for date, value in zip(dates, [1.1, 1.2]):
            rows.append({"series_code": series_code, "timestamp": date, "value": value})
    return pd.DataFrame(rows)


class FakeRewriteDataAPIResource:
    def __init__(self, metadata: pd.DataFrame, values: pd.DataFrame):
        self.api = create_data_api(
            duckdb_connection=object(),
            metadata_repository=FakeMetadataStorage(metadata),
            value_repository=FakeValueStorage(values),
        )
        DatasetBase.configure(self.api)


def _strategy_config() -> StrategyConfig:
    return StrategyConfig(
        universe=UNIVERSE,
        ticker_source="bloomberg",
        window_months=12,
        stop_reward_ratio=2.0,
        logged_rate_threshold=0.01,
        min_observations=60,
        global_equity_series="IDX0005_INDEX",
        commodity_series="XAU_PX_0032",
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": 1,
            "global_equity": 1,
            "commodity": 1,
        },
    )


class FakeSteerConfigResource:
    def __init__(self, *configs: StrategyConfig):
        self._configs = {config.universe: config for config in configs}

    def for_universe(self, universe: str) -> StrategyConfig:
        return self._configs[universe]


class FakeSteerCatalogResource:
    def __init__(self, catalog: SteerCatalog):
        self.catalog = catalog


@pytest.fixture
def in_memory_catalog() -> SteerCatalog:
    catalog = SteerCatalog(duckdb.connect(":memory:"))
    catalog.ensure_schemas()
    return catalog


@pytest.fixture
def resources(in_memory_catalog: SteerCatalog):
    return {
        "rewrite_data_api": FakeRewriteDataAPIResource(METADATA, _values_frame()),
        "steer_config": FakeSteerConfigResource(_strategy_config()),
        "steer_catalog": FakeSteerCatalogResource(in_memory_catalog),
    }


def test_full_graph_processes_every_pair_and_skips_cleanly_when_blocked(
    resources, in_memory_catalog
):
    """local_equity is never available (see steer/discovery.py) -- every pair in the universe is blocked,
    and the whole chain should skip cleanly end to end rather than fail, with nothing written to the
    gold tables. Both pairs in the universe are discovered and processed within ONE partition run."""
    result = materialize(
        [
            steer_data_availability,
            steer_silver_prices,
            steer_features,
            steer_cointegration,
            steer_estimate,
            steer_signal,
        ],
        resources=resources,
        partition_key=UNIVERSE,
        instance=DagsterInstance.ephemeral(),
    )

    assert result.success

    availability_output = result.output_for_node("steer_data_availability", output_name="result")
    assert len(availability_output) == len(PAIRS)
    assert availability_output["blocked"].all()

    silver_output = result.output_for_node("steer_silver_prices", output_name="result")
    assert isinstance(silver_output, pd.DataFrame)
    assert silver_output.empty

    signal_output = result.output_for_node("steer_signal", output_name="result")
    assert isinstance(signal_output, pd.DataFrame)
    assert signal_output.empty

    estimates_table = in_memory_catalog.read(GOLD_SCHEMA, STEER_ESTIMATES_TABLE)
    signals_table = in_memory_catalog.read(GOLD_SCHEMA, STEER_SIGNALS_TABLE)
    assert estimates_table.empty
    assert signals_table.empty


def test_data_availability_reports_every_pair_in_the_universe(resources):
    result = materialize(
        [steer_data_availability],
        resources={"rewrite_data_api": resources["rewrite_data_api"]},
        partition_key=UNIVERSE,
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success

    report = result.output_for_node("steer_data_availability", output_name="result")
    assert set(report["series_code"]) == set(PAIRS)
    assert (report["universe"] == UNIVERSE).all()
    assert report["blocked"].all()
    assert report["local_equity_available"].eq(False).all()


def test_silver_prices_discovers_and_skips_every_pair_as_blocked(resources):
    result = materialize(
        [steer_silver_prices],
        resources={
            "rewrite_data_api": resources["rewrite_data_api"],
            "steer_config": resources["steer_config"],
        },
        partition_key=UNIVERSE,
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success

    materializations = [
        event for event in result.all_events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    metadata = materializations[-1].materialization.metadata
    assert metadata["pair_count"].value == len(PAIRS)
    assert metadata["blocked_pair_count"].value == len(PAIRS)
    assert metadata["fetched_pair_count"].value == 0
