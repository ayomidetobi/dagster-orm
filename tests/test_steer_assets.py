"""Dagster-level tests for the STEER asset graph, with injected fake upstream data.

Uses dagster.materialize() (per the tooling constraints -- pytest + an
in-memory DuckDB fixture + Dagster's own testing utilities) rather than
mocking. rewrite_data_api is the REAL DataAPI (rewrite.data_api.factory.create_data_api)
wired to fake in-memory metadata/value storage repositories, but a REAL
in-memory duckdb connection for `duckdb_connection` -- this exercises the
real QuerySet/MetadataService/ValueService stack the FX datasets
(FXDevelopedMarkets etc.) depend on via .query(), not just get_metadata()/
get_values() in isolation, and gives DataAPI.read_table()/.write_table()
(steer_estimate/steer_signal's gold.steer_estimates/gold.steer_signals
writes) somewhere real to read/write.

METADATA/PAIRS below deliberately have no role-filter coverage (see
steer/config.py's STEER_AVAILABILITY_SPEC.role_filters) for either pair's
legs, so every pair in that fixture stays blocked -- exercising the
per-variant loop and the blocking/skip path, which is real, verified
behavior. The role-resolution logic itself (assess_pair_availability --
which roles/legs are required per variant, non-USD-leg-only roles, etc.)
is covered directly by tests/test_availability_report.py's pure-function
tests instead.
test_full_graph_produces_an_estimate_and_signal_for_a_fully_available_pair
below is the opposite case: a fully-resolvable G10 pair, proving the new
metadata-driven discovery + driver construction actually reaches a written
gold.steer_estimates/gold.steer_signals row, not just that it degrades
gracefully when data is missing.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pytest
from dagster import DagsterInstance, materialize

from dagster_quickstart.assets.availability_asset import fx_data_availability
from dagster_quickstart.assets.steer.cointegration_asset import steer_cointegration
from dagster_quickstart.assets.steer.estimate_asset import steer_estimate
from dagster_quickstart.assets.steer.gold_features_asset import steer_features
from dagster_quickstart.assets.steer.signal_asset import steer_signal
from dagster_quickstart.assets.steer.silver_asset import steer_silver_prices
from dagster_quickstart.rewrite.data_api.dataset import DatasetBase
from dagster_quickstart.rewrite.data_api.factory import create_data_api
from dagster_quickstart.steer.config import StrategyConfig
from dagster_quickstart.steer.orm import GOLD_SCHEMA, STEER_ESTIMATES_TABLE, STEER_SIGNALS_TABLE

VARIANT = "G10"
PAIRS = ["AUDJPY_PX_LAST", "EURGBP_PX_LAST"]

#: FX-pair rows have no role-filter columns filled in -- deliberately, so
#: every pair here stays blocked (see test_full_graph_processes_every_pair_and_skips_cleanly_when_blocked).
METADATA = pd.DataFrame(
    {
        "series_code": PAIRS
        + ["US2Y_YIELD_0021", "JP5Y_YIELD_0047", "MXWO_PX_LAST", "BRENT_PX_LAST"],
        "asset_class": [
            "Currency",
            "Currency",
            "Fixed Income",
            "Fixed Income",
            "Equity",
            "Commodity",
        ],
        "sub_asset_class": ["FX Spot", "FX Spot", None, None, None, None],
        "market_development": ["G10", "G10", "G10", "G10", "GLOBAL", "GLOBAL"],
        "currency": ["AUD", "EUR", "USD", "JPY", "USD", "USD"],
        "tenor": [None, None, None, None, None, None],
        "market_segment": [None, None, None, None, None, None],
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
        "MXWO_PX_LAST",
        "BRENT_PX_LAST",
    ]:
        for date, value in zip(dates, [1.1, 1.2]):
            rows.append({"series_code": series_code, "timestamp": date, "value": value})
    return pd.DataFrame(rows)


class FakeRewriteDataAPIResource:
    def __init__(self, metadata: pd.DataFrame, values: pd.DataFrame):
        self.api = create_data_api(
            duckdb_connection=duckdb.connect(":memory:"),
            metadata_repository=FakeMetadataStorage(metadata),
            value_repository=FakeValueStorage(values),
        )
        DatasetBase.configure(self.api)


def _strategy_config() -> StrategyConfig:
    return StrategyConfig(
        variant=VARIANT,
        ticker_source="bloomberg",
        window_months=12,
        stop_reward_ratio=2.0,
        logged_rate_threshold=0.01,
        min_observations=60,
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
        self._configs = {config.variant: config for config in configs}

    def for_variant(self, variant: str) -> StrategyConfig:
        return self._configs[variant]


@pytest.fixture
def resources():
    return {
        "rewrite_data_api": FakeRewriteDataAPIResource(METADATA, _values_frame()),
        "steer_config": FakeSteerConfigResource(_strategy_config()),
    }


def test_full_graph_processes_every_pair_and_skips_cleanly_when_blocked(resources):
    """local_equity is never available (see steer/discovery.py) -- every pair in the variant is blocked,
    and the whole chain should skip cleanly end to end rather than fail, with nothing written to the
    gold tables. Both pairs in the variant are discovered and processed within ONE partition run."""
    result = materialize(
        [
            fx_data_availability,
            steer_silver_prices,
            steer_features,
            steer_cointegration,
            steer_estimate,
            steer_signal,
        ],
        resources=resources,
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )

    assert result.success

    availability_output = result.output_for_node("fx_data_availability", output_name="result")
    assert len(availability_output) == len(PAIRS)
    assert availability_output["blocked"].all()

    silver_output = result.output_for_node("steer_silver_prices", output_name="result")
    assert isinstance(silver_output, pd.DataFrame)
    assert silver_output.empty

    signal_output = result.output_for_node("steer_signal", output_name="result")
    assert isinstance(signal_output, pd.DataFrame)
    assert signal_output.empty

    data_api = resources["rewrite_data_api"].api
    estimates_table = data_api.read_table(GOLD_SCHEMA, STEER_ESTIMATES_TABLE).frame
    signals_table = data_api.read_table(GOLD_SCHEMA, STEER_SIGNALS_TABLE).frame
    assert estimates_table.empty
    assert signals_table.empty


def test_data_availability_reports_every_pair_in_the_variant(resources):
    result = materialize(
        [fx_data_availability],
        resources={"rewrite_data_api": resources["rewrite_data_api"]},
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success

    report = result.output_for_node("fx_data_availability", output_name="result")
    assert set(report["series_code"]) == set(PAIRS)
    assert (report["variant"] == VARIANT).all()
    assert report["blocked"].all()
    assert report["block_reasons"].str.contains("local_equity").all()


def test_silver_prices_discovers_and_skips_every_pair_as_blocked(resources):
    result = materialize(
        [fx_data_availability, steer_silver_prices],
        resources={
            "rewrite_data_api": resources["rewrite_data_api"],
            "steer_config": resources["steer_config"],
        },
        partition_key=VARIANT,
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


def _unblocked_g10_metadata() -> pd.DataFrame:
    """One G10 pair (EURUSD) with every required role resolvable -- swap_2y/
    rate_3m/yield_10y/local_equity for both EUR and USD -- so it clears
    fx_data_availability and flows all the way through to a signal."""
    rows = [
        {
            "series_code": "EURUSD_PX_LAST",
            "asset_class": "Currency",
            "sub_asset_class": "FX Spot",
            "market_development": "G10",
            "currency": "EUR",
            "tenor": None,
            "market_segment": None,
        },
        {
            "series_code": "MXWO_PX_LAST",
            "asset_class": "Equity",
            "sub_asset_class": "Equity Index",
            "market_development": "GLOBAL",
            "currency": "USD",
            "tenor": None,
            "market_segment": "Global",
        },
        {
            "series_code": "BRENT_PX_LAST",
            "asset_class": "Commodity",
            "sub_asset_class": "Crude Oil",
            "market_development": "GLOBAL",
            "currency": "USD",
            "tenor": None,
            "market_segment": None,
        },
    ]
    for ccy in ("EUR", "USD"):
        rows.append(
            {
                "series_code": f"{ccy}_SWAP",
                "asset_class": "Fixed Income",
                "sub_asset_class": "Interest Rate Swap",
                "market_development": "G10",
                "currency": ccy,
                "tenor": "2Y",
                "market_segment": None,
            }
        )
        rows.append(
            {
                "series_code": f"{ccy}_3M",
                "asset_class": "Fixed Income",
                "sub_asset_class": "Money Market Rate",
                "market_development": "G10",
                "currency": ccy,
                "tenor": "3M",
                "market_segment": None,
            }
        )
        rows.append(
            {
                "series_code": f"{ccy}_10Y",
                "asset_class": "Fixed Income",
                "sub_asset_class": "Sovereign Yield",
                "market_development": "G10",
                "currency": ccy,
                "tenor": "10Y",
                "market_segment": None,
            }
        )
        rows.append(
            {
                "series_code": f"{ccy}_EQ",
                "asset_class": "Equity",
                "sub_asset_class": "Equity Index",
                "market_development": "G10",
                "currency": ccy,
                "tenor": None,
                "market_segment": "Local",
            }
        )
    return pd.DataFrame(rows)


def _unblocked_g10_values() -> pd.DataFrame:
    rng = np.random.default_rng(21)
    n = 400
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=n)

    eur_swap = 2.0 + np.cumsum(rng.normal(0, 0.01, n))
    usd_swap = 1.5 + np.cumsum(rng.normal(0, 0.01, n))
    eur_3m = 2.1 + np.cumsum(rng.normal(0, 0.01, n))
    usd_3m = 1.4 + np.cumsum(rng.normal(0, 0.01, n))
    eur_10y = 3.0 + np.cumsum(rng.normal(0, 0.01, n))
    usd_10y = 2.6 + np.cumsum(rng.normal(0, 0.01, n))
    eur_eq = 800 + np.cumsum(rng.normal(0, 2, n))
    usd_eq = 2500 + np.cumsum(rng.normal(0, 5, n))
    mxwo = 100 + np.cumsum(rng.normal(0, 0.5, n))
    brent = 80 + np.cumsum(rng.normal(0, 0.3, n))

    ird = eur_swap - usd_swap
    curve = (eur_3m - eur_10y) - (usd_3m - usd_10y)
    local_eq = np.log(eur_eq) - np.log(usd_eq)
    rate = 1.1 + 0.05 * ird - 0.02 * curve + 0.05 * local_eq + rng.normal(0, 0.005, n)

    series = {
        "EURUSD_PX_LAST": rate,
        "EUR_SWAP": eur_swap,
        "USD_SWAP": usd_swap,
        "EUR_3M": eur_3m,
        "USD_3M": usd_3m,
        "EUR_10Y": eur_10y,
        "USD_10Y": usd_10y,
        "EUR_EQ": eur_eq,
        "USD_EQ": usd_eq,
        "MXWO_PX_LAST": mxwo,
        "BRENT_PX_LAST": brent,
    }
    rows = []
    for series_code, values in series.items():
        for date, value in zip(dates, values):
            rows.append({"series_code": series_code, "timestamp": date, "value": float(value)})
    return pd.DataFrame(rows)


def test_full_graph_produces_an_estimate_and_signal_for_a_fully_available_pair():
    """The full opposite of the blocked-pair test: every required role
    resolves for EURUSD, so the pair should flow all the way through to a
    written gold.steer_estimates / gold.steer_signals row -- proving the
    new metadata-driven discovery + driver construction actually works end
    to end, not just that it degrades gracefully when data is missing."""
    resources = {
        "rewrite_data_api": FakeRewriteDataAPIResource(
            _unblocked_g10_metadata(), _unblocked_g10_values()
        ),
        "steer_config": FakeSteerConfigResource(_strategy_config()),
    }

    result = materialize(
        [
            fx_data_availability,
            steer_silver_prices,
            steer_features,
            steer_cointegration,
            steer_estimate,
            steer_signal,
        ],
        resources=resources,
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )

    assert result.success

    availability_output = result.output_for_node("fx_data_availability", output_name="result")
    assert availability_output["blocked"].eq(False).all()

    silver_output = result.output_for_node("steer_silver_prices", output_name="result")
    assert not silver_output.empty

    estimate_output = result.output_for_node("steer_estimate", output_name="result")
    assert not estimate_output.empty
    assert estimate_output.iloc[0]["series_code"] == "EURUSD_PX_LAST"

    signal_output = result.output_for_node("steer_signal", output_name="result")
    assert not signal_output.empty
    assert signal_output.iloc[0]["signal"] in {"BUY", "SELL", "NONE"}

    data_api = resources["rewrite_data_api"].api
    estimates_table = data_api.read_table(GOLD_SCHEMA, STEER_ESTIMATES_TABLE).frame
    signals_table = data_api.read_table(GOLD_SCHEMA, STEER_SIGNALS_TABLE).frame
    assert not estimates_table.empty
    assert not signals_table.empty

    # Acceptance criterion 6: the persisted column is (deliberately) still "universe", not
    # "variant" -- see steer/orm.py's module docstring. gold.steer_estimates/gold.steer_signals
    # already hold real rows under "universe"; a stray "variant" column would mean a write
    # somewhere in estimate_asset.py/signal_asset.py regressed back to the Python-level name.
    assert "universe" in estimates_table.columns
    assert "variant" not in estimates_table.columns
    assert "universe" in signals_table.columns
    assert "variant" not in signals_table.columns


def _wrap_with_call_counter(api, method_name: str) -> dict:
    """Monkeypatch `api.<method_name>` to count calls; returns a dict with a live "count" key."""
    original = getattr(api, method_name)
    counter = {"count": 0}

    def wrapper(*args, **kwargs):
        counter["count"] += 1
        return original(*args, **kwargs)

    setattr(api, method_name, wrapper)
    return counter


def test_steer_silver_prices_issues_zero_get_metadata_calls_for_role_resolution(resources):
    """Acceptance criterion: steer_silver_prices depends on fx_data_availability's output
    (see silver_asset.py) and reconstructs every pair's PairAvailability from it via
    PairAvailability.from_report_row -- no re-discovery, no re-resolution. Proven black-box:
    adding steer_silver_prices to the materialize() selection must not change the
    get_metadata() call count versus materializing fx_data_availability alone."""
    availability_only_counter = _wrap_with_call_counter(
        resources["rewrite_data_api"].api, "get_metadata"
    )
    materialize(
        [fx_data_availability],
        resources={"rewrite_data_api": resources["rewrite_data_api"]},
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )
    availability_only_calls = availability_only_counter["count"]

    both_resources = {
        "rewrite_data_api": FakeRewriteDataAPIResource(METADATA, _values_frame()),
        "steer_config": resources["steer_config"],
    }
    both_counter = _wrap_with_call_counter(both_resources["rewrite_data_api"].api, "get_metadata")
    materialize(
        [fx_data_availability, steer_silver_prices],
        resources=both_resources,
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )

    assert both_counter["count"] == availability_only_calls


def test_blocked_pair_log_lines_are_unchanged_in_wording_and_count(resources, capfd):
    """Acceptance criterion: build_silver_frame returns skipped_reasons instead of logging
    itself, and steer_silver_prices logs each one -- this proves the resulting log lines are
    byte-for-byte what the asset used to emit inline, for both of this fixture's blocked pairs.

    Captures at the OS file-descriptor level (capfd) rather than via a Python logging.Handler
    (caplog, or a manually attached one) -- Dagster's own instance/run setup reconfigures the
    "dagster" logger's handlers in a way neither reliably survives; capfd doesn't care what
    wrote to stderr, only that something did.
    """
    materialize(
        [fx_data_availability, steer_silver_prices],
        resources=resources,
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )

    captured_err = capfd.readouterr().err
    skip_lines = [line for line in captured_err.splitlines() if "Skipping" in line]

    assert len(skip_lines) == len(PAIRS)
    for series_code in PAIRS:
        expected = f"Skipping {series_code} -- blocked: "
        assert any(expected in line for line in skip_lines), captured_err


def test_silver_asset_output_matches_build_silver_frame_called_directly(resources):
    """Acceptance criterion: the asset's Output frame and check metadata are byte-identical to
    calling steer.pipeline.build_silver_frame directly on the same inputs -- steer_silver_prices
    is proven to be a thin wrapper, not an independent implementation."""
    from dagster_quickstart.availability.report import pairs_from_availability_report
    from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC
    from dagster_quickstart.steer.source.features import build_silver_frame

    result = materialize(
        [fx_data_availability, steer_silver_prices],
        resources=resources,
        partition_key=VARIANT,
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success

    availability_output = result.output_for_node("fx_data_availability", output_name="result")
    asset_silver_output = result.output_for_node("steer_silver_prices", output_name="result")
    materializations = [
        event
        for event in result.all_events
        if event.event_type_value == "ASSET_MATERIALIZATION"
        and event.step_key == "steer_silver_prices"
    ]
    asset_metadata = materializations[-1].materialization.metadata

    availabilities = pairs_from_availability_report(availability_output, STEER_AVAILABILITY_SPEC)
    as_of = pd.Timestamp.utcnow().tz_localize(None).normalize()
    direct_result = build_silver_frame(
        resources["rewrite_data_api"].api,
        VARIANT,
        resources["steer_config"].for_variant(VARIANT),
        availabilities,
        as_of=as_of,
    )

    pd.testing.assert_frame_equal(asset_silver_output, direct_result.frame)
    assert asset_metadata["pair_count"].value == direct_result.pair_count
    assert asset_metadata["fetched_pair_count"].value == direct_result.fetched_pair_count
    assert asset_metadata["blocked_pair_count"].value == len(direct_result.blocked_pairs)
    assert asset_metadata["stale_pair_count"].value == len(direct_result.stale_pairs)
    assert asset_metadata["row_count"].value == len(direct_result.frame)
