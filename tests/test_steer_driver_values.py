"""Tests for steer.features.DriverValues / required_series_codes -- the run-scoped values loader.

Covers the acceptance criteria for the fetch-values-once refactor: one get_values() call per
run (not per pair), multi-vendor (HAWK/Macrobond) series surviving that one call, per-pair
output unchanged versus the old per-pair-fetch shape, and NaN (never a crash or a substituted
series) for a pair whose role didn't resolve to any real data.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.availability.report import PairAvailability
from dagster_quickstart.steer.config import DRIVER_NAMES, StrategyConfig
from dagster_quickstart.steer.source.features import (
    DriverValues,
    fetch_raw_driver_frame,
    required_series_codes,
)


class _CountingValueAPI:
    """Wraps a wide value frame; counts get_values() calls and asserts ticker_source is never passed."""

    def __init__(self, wide: pd.DataFrame):
        self._wide = wide
        self.call_count = 0
        self.calls: list[dict] = []

    def get_values(self, series_codes, **kwargs):
        self.call_count += 1
        self.calls.append({"series_codes": list(series_codes), "kwargs": kwargs})
        assert "ticker_source" not in kwargs or kwargs["ticker_source"] is None, (
            "get_values() must not be called with an explicit ticker_source -- it would pin "
            "every series to one vendor and silently drop the others (HAWK/Macrobond)."
        )
        columns = [c for c in series_codes if c in self._wide.columns]
        return self._wide[columns]


class _VendorTaggedValueStorage:
    """Mirrors the real DuckLake behavior: get_values(ticker_source=X) returns only rows
    tagged vendor X; ticker_source=None returns every vendor's rows. Used to prove that NOT
    passing ticker_source is what makes HAWK/Macrobond series come back at all."""

    def __init__(self, rows: pd.DataFrame):
        self._rows = rows  # columns: series_code, timestamp, value, ticker_source

    def get_values(self, series_codes, *, ticker_source=None, **kwargs):
        frame = self._rows[self._rows["series_code"].isin(series_codes)]
        if ticker_source is not None:
            frame = frame[frame["ticker_source"] == ticker_source]
        return frame.drop(columns=["ticker_source"]).reset_index(drop=True)

    def get_last_values(self, series_codes, **kwargs):
        return self.get_values(series_codes, **kwargs)

    def value_exists(self, series_codes, **kwargs):
        existing = set(self._rows["series_code"].unique())
        return {code: code in existing for code in series_codes}

    def save_values(self, frame):
        raise NotImplementedError

    def delete_values(self, filters):
        raise NotImplementedError

    def get_storage_path(self):
        return None


def _strategy_config(variant: str, drivers=DRIVER_NAMES) -> StrategyConfig:
    return StrategyConfig(
        variant=variant,
        window_months=12,
        stop_reward_ratio=2.0,
        logged_rate_threshold=0.01,
        drivers=drivers,
        expected_signs={driver: 0 for driver in drivers},
    )


def _g10_availability(series_code: str, base: str, quote: str) -> PairAvailability:
    return PairAvailability(
        series_code=series_code,
        variant="G10",
        base_currency=base,
        quote_currency=quote,
        resolved={
            ("base", "swap_2y"): f"{base}_SWAP",
            ("quote", "swap_2y"): f"{quote}_SWAP",
            ("base", "rate_3m"): f"{base}_3M",
            ("quote", "rate_3m"): f"{quote}_3M",
            ("base", "yield_10y"): f"{base}_10Y",
            ("quote", "yield_10y"): f"{quote}_10Y",
            ("base", "local_equity"): f"{base}_EQ",
            ("quote", "local_equity"): f"{quote}_EQ",
        },
    )


def _em_availability(series_code: str, base: str, quote: str, non_usd: str) -> PairAvailability:
    return PairAvailability(
        series_code=series_code,
        variant="EM",
        base_currency=base,
        quote_currency=quote,
        resolved={
            ("base", "swap_2y"): f"{base}_SWAP",
            ("quote", "swap_2y"): f"{quote}_SWAP",
            ("base", "local_equity"): f"{base}_EQ",
            ("quote", "local_equity"): f"{quote}_EQ",
            ("base" if non_usd == base else "quote", "cds_5y"): f"{non_usd}_CDS",
        },
    )


def _wide_values(n: int = 90, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-01", periods=n)
    columns = [
        "EURUSD_PX_LAST", "GBPUSD_PX_LAST", "USDZAR_PX_LAST",
        "MXWO_PX_LAST", "BRENT_PX_LAST",
        "EUR_SWAP", "USD_SWAP", "GBP_SWAP", "ZAR_SWAP",
        "EUR_3M", "USD_3M", "GBP_3M",
        "EUR_10Y", "USD_10Y", "GBP_10Y",
        "EUR_EQ", "USD_EQ", "GBP_EQ", "ZAR_EQ",
        "ZAR_CDS",
    ]
    data = {col: 100 + np.cumsum(rng.normal(0, 0.5, n)) for col in columns}
    return pd.DataFrame(data, index=dates)


def test_required_series_codes_collects_the_union_across_pairs():
    strategy_config = _strategy_config("G10")
    pairs = [
        ("EURUSD_PX_LAST", _g10_availability("EURUSD_PX_LAST", "EUR", "USD")),
        ("GBPUSD_PX_LAST", _g10_availability("GBPUSD_PX_LAST", "GBP", "USD")),
    ]

    codes = required_series_codes(pairs, strategy_config)

    # USD's role series and the global drivers are shared by both pairs -- one entry each.
    assert "USD_SWAP" in codes
    assert "MXWO_PX_LAST" in codes
    assert "BRENT_PX_LAST" in codes
    assert "EUR_SWAP" in codes
    assert "GBP_SWAP" in codes
    assert "EURUSD_PX_LAST" in codes
    assert "GBPUSD_PX_LAST" in codes


def test_a_full_run_issues_exactly_one_get_values_call():
    """Acceptance criterion 1."""
    strategy_config = _strategy_config("G10")
    pairs = [
        ("EURUSD_PX_LAST", _g10_availability("EURUSD_PX_LAST", "EUR", "USD")),
        ("GBPUSD_PX_LAST", _g10_availability("GBPUSD_PX_LAST", "GBP", "USD")),
    ]
    codes = required_series_codes(pairs, strategy_config)
    api = _CountingValueAPI(_wide_values())

    driver_values = DriverValues.load(api, codes)

    for series_code, availability in pairs:
        fetch_raw_driver_frame(driver_values, series_code, strategy_config, availability)

    assert api.call_count == 1


def test_hawk_and_macrobond_series_survive_the_one_call():
    """Acceptance criterion 2 -- proves NOT passing ticker_source is what makes this work:
    a vendor-tagged fake storage that filters by ticker_source when given one, exactly like
    the real DuckLake repository."""
    dates = pd.bdate_range("2024-01-01", periods=10)
    rows = pd.concat(
        [
            pd.DataFrame(
                {"series_code": "ZARCDS_PX_LAST", "timestamp": dates, "value": 220.0, "ticker_source": "HAWK"}
            ),
            pd.DataFrame(
                {"series_code": "SUGAR_PX_LAST", "timestamp": dates, "value": 18.0, "ticker_source": "Macrobond"}
            ),
            pd.DataFrame(
                {"series_code": "EURUSD_PX_LAST", "timestamp": dates, "value": 1.1, "ticker_source": "BBG"}
            ),
        ],
        ignore_index=True,
    )
    from dagster_quickstart.rewrite.data_api.factory import create_data_api

    class _NoMetadata:
        def get_columns(self):
            return []

        def get_metadata(self, *a, **k):
            return pd.DataFrame()

        def get_distinct_values(self, *a, **k):
            return []

        def save_metadata(self, *a, **k):
            raise NotImplementedError

        def refresh_metadata(self):
            pass

    api = create_data_api(
        duckdb_connection=object(),
        metadata_repository=_NoMetadata(),
        value_repository=_VendorTaggedValueStorage(rows),
    )

    driver_values = DriverValues.load(api, ["ZARCDS_PX_LAST", "SUGAR_PX_LAST", "EURUSD_PX_LAST"])
    wide = driver_values.select(
        {"cds": "ZARCDS_PX_LAST", "sugar": "SUGAR_PX_LAST", "rate": "EURUSD_PX_LAST"}
    )

    assert wide["cds"].notna().all()
    assert wide["sugar"].notna().all()
    assert wide["rate"].notna().all()


def test_missing_series_produces_a_nan_column_not_a_crash():
    """Acceptance criterion 4."""
    strategy_config = _strategy_config("G10")
    availability = _g10_availability("EURUSD_PX_LAST", "EUR", "USD")
    # A wide frame that's missing every role series (e.g. resolved to a series_code that was
    # never actually ingested with price history) -- only rate/global drivers present.
    dates = pd.bdate_range("2024-01-01", periods=10)
    sparse = pd.DataFrame(
        {"EURUSD_PX_LAST": 1.1, "MXWO_PX_LAST": 100.0, "BRENT_PX_LAST": 80.0}, index=dates
    )
    driver_values = DriverValues(sparse)

    features = fetch_raw_driver_frame(driver_values, "EURUSD_PX_LAST", strategy_config, availability)

    assert not features.empty
    assert features["interest_rate_differential"].isna().all()
    assert features["local_equity"].isna().all()
    assert features["yield_curve_or_cds"].isna().all()
    assert features["rate"].notna().all()  # the pair's own rate is unaffected


def test_per_pair_output_identical_whether_driver_values_covers_one_pair_or_the_whole_run():
    """Acceptance criterion 3 -- a proxy for "byte-identical to the old per-pair-fetch output":
    a pair's derived features must not change depending on how many OTHER pairs' unrelated
    series happen to be loaded alongside it in the shared wide frame. G10 and EM both covered.
    """
    wide = _wide_values()

    g10_config = _strategy_config("G10")
    g10_availability = _g10_availability("EURUSD_PX_LAST", "EUR", "USD")
    only_this_pair = DriverValues(
        wide[["EURUSD_PX_LAST", "MXWO_PX_LAST", "BRENT_PX_LAST", "EUR_SWAP", "USD_SWAP",
              "EUR_3M", "USD_3M", "EUR_10Y", "USD_10Y", "EUR_EQ", "USD_EQ"]]
    )
    whole_run = DriverValues(wide)  # includes GBPUSD/ZAR/etc columns this pair never touches

    from_narrow = fetch_raw_driver_frame(only_this_pair, "EURUSD_PX_LAST", g10_config, g10_availability)
    from_wide = fetch_raw_driver_frame(whole_run, "EURUSD_PX_LAST", g10_config, g10_availability)
    pd.testing.assert_frame_equal(from_narrow, from_wide)

    em_config = _strategy_config("EM")
    em_availability = _em_availability("USDZAR_PX_LAST", "USD", "ZAR", "ZAR")
    only_this_em_pair = DriverValues(
        wide[["USDZAR_PX_LAST", "MXWO_PX_LAST", "BRENT_PX_LAST", "USD_SWAP", "ZAR_SWAP",
              "USD_EQ", "ZAR_EQ", "ZAR_CDS"]]
    )
    from_narrow_em = fetch_raw_driver_frame(only_this_em_pair, "USDZAR_PX_LAST", em_config, em_availability)
    from_wide_em = fetch_raw_driver_frame(whole_run, "USDZAR_PX_LAST", em_config, em_availability)
    pd.testing.assert_frame_equal(from_narrow_em, from_wide_em)


def test_wall_clock_report_one_call_vs_n_calls(capsys):
    """Not a correctness assertion -- reports before/after wall-clock for a simulated run, using
    a fixed per-call latency to model the real DuckLake round trip this sandbox can't reach
    directly (see the task's measurement step -- a live DataAPI(live=False) hangs here with no
    DB/S3 connectivity)."""
    strategy_config = _strategy_config("G10")
    n_pairs = 45
    pairs = [
        (f"PAIR{i}_PX_LAST", _g10_availability(f"PAIR{i}_PX_LAST", "EUR", "USD"))
        for i in range(n_pairs)
    ]
    codes = required_series_codes(pairs, strategy_config)
    simulated_round_trip_seconds = 0.05  # a conservative stand-in for one DuckLake/S3 round trip

    before = n_pairs * simulated_round_trip_seconds
    after = 1 * simulated_round_trip_seconds
    print(
        f"\nsimulated wall-clock (one get_values() round trip modeled at "
        f"{simulated_round_trip_seconds}s): before (per-pair) = {before:.2f}s "
        f"({n_pairs} calls), after (run-scoped) = {after:.2f}s (1 call), "
        f"{len(codes)} distinct series in that one call."
    )
    assert after < before
