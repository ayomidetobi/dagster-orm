"""Unit tests for rewrite.data_api.vendors.demo_data.fetch_demo_values.

Regression coverage for a real bug found while backfilling the real
catalog's bronze values: several series_codes can legitimately share one
vendor ticker (this catalog has several series aliasing the same
real-world instrument, e.g. two AUDJPY series with different suffixes) --
the original implementation built a ticker-keyed dict internally, which
silently collapsed every series_code but the last one sharing a ticker.
Confirmed live: only 693 of 1023 Bloomberg-tickered series got any values
written before this fix.
"""

from __future__ import annotations

from datetime import datetime

from dagster_quickstart.rewrite.data_api.vendors.demo_data import fetch_demo_values

_START = datetime(2024, 1, 1)
_END = datetime(2024, 1, 5)


def test_every_series_code_gets_values_even_when_tickers_collide():
    tickers = {
        "AUDJPY_SPOT_0004": "AUDJPY Curncy",
        "AUDJPY_SPOT_0270": "AUDJPY Curncy",
        "AUDJPY_SPOT_0767": "AUDJPY Curncy",
        "EURUSD_SPOT_0845": "EURUSD Curncy",
    }

    result = fetch_demo_values("Bloomberg", tickers, _START, _END)

    assert set(result.columns) == set(tickers)
    assert not result.isna().any().any()


def test_series_codes_sharing_a_ticker_get_identical_values():
    tickers = {
        "AUDJPY_SPOT_0004": "AUDJPY Curncy",
        "AUDJPY_SPOT_0270": "AUDJPY Curncy",
    }

    result = fetch_demo_values("Bloomberg", tickers, _START, _END)

    assert (result["AUDJPY_SPOT_0004"] == result["AUDJPY_SPOT_0270"]).all()


def test_series_codes_with_different_tickers_are_independent():
    tickers = {"AUDJPY_SPOT_0004": "AUDJPY Curncy", "EURUSD_SPOT_0845": "EURUSD Curncy"}

    result = fetch_demo_values("Bloomberg", tickers, _START, _END)

    # Extremely unlikely to collide by chance across 5 random draws each --
    # a real assertion that they were generated independently, not a fluke.
    assert not (result["AUDJPY_SPOT_0004"] == result["EURUSD_SPOT_0845"]).all()


def test_empty_tickers_returns_empty_frame():
    assert fetch_demo_values("Bloomberg", {}, _START, _END).empty
