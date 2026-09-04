"""Unit tests for steer.pipeline.build_silver_frame -- no Dagster involved anywhere.

Acceptance criterion: build_silver_frame is callable directly with a stub data_api, proving
the silver pipeline is genuinely library code, not something that only works wired into an
@asset function.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.steer.config import DRIVER_NAMES, StrategyConfig
from dagster_quickstart.availability.report import PairAvailability
from dagster_quickstart.steer.source.features import SERIES_CODE_COLUMN, build_silver_frame


class _StubDataAPI:
    """Minimal get_values/get_metadata stand-in -- no Dagster, no DuckLake."""

    def __init__(self, values: pd.DataFrame, metadata: pd.DataFrame | None = None):
        self._values = values
        self._metadata = (
            metadata if metadata is not None else pd.DataFrame(columns=["series_code", "valid_to"])
        )

    def get_values(self, series_codes, **kwargs):
        columns = [c for c in series_codes if c in self._values.columns]
        return self._values[columns]

    def get_metadata(self, **filters):
        frame = self._metadata
        if "series_code" in filters:
            frame = frame[frame["series_code"].isin(filters["series_code"])]
        return type("Result", (), {"frame": frame.reset_index(drop=True)})()


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


def _blocked_availability(series_code: str) -> PairAvailability:
    return PairAvailability(
        series_code=series_code,
        variant="G10",
        base_currency="AUD",
        quote_currency="USD",
        missing_reasons={"base:swap_2y": "No swap_2y series for AUD."},
    )


def _fresh_wide_values(as_of: pd.Timestamp, n: int = 90) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    dates = pd.bdate_range(end=as_of, periods=n)
    columns = [
        "EURUSD_PX_LAST",
        "MXWO_PX_LAST",
        "BRENT_PX_LAST",
        "EUR_SWAP",
        "USD_SWAP",
        "EUR_3M",
        "USD_3M",
        "EUR_10Y",
        "USD_10Y",
        "EUR_EQ",
        "USD_EQ",
    ]
    data = {col: 100 + np.cumsum(rng.normal(0, 0.5, n)) for col in columns}
    return pd.DataFrame(data, index=dates)


def test_build_silver_frame_is_callable_with_a_stub_data_api_no_dagster():
    """The acceptance criterion, literally: this test imports nothing from dagster."""
    as_of = pd.Timestamp("2024-06-01")
    availability = _g10_availability("EURUSD_PX_LAST", "EUR", "USD")
    api = _StubDataAPI(_fresh_wide_values(as_of))

    result = build_silver_frame(api, "G10", _strategy_config("G10"), [availability], as_of=as_of)

    assert not result.frame.empty
    assert result.pair_count == 1
    assert result.fetched_pair_count == 1
    assert result.blocked_pairs == []
    assert result.stale_pairs == []
    assert (result.frame[SERIES_CODE_COLUMN] == "EURUSD_PX_LAST").all()


def test_blocked_pair_is_skipped_and_recorded():
    as_of = pd.Timestamp("2024-06-01")
    availability = _blocked_availability("AUDUSD_PX_LAST")
    api = _StubDataAPI(pd.DataFrame())

    result = build_silver_frame(api, "G10", _strategy_config("G10"), [availability], as_of=as_of)

    assert result.frame.empty
    assert result.fetched_pair_count == 0
    assert result.blocked_pairs == ["AUDUSD_PX_LAST"]
    assert result.stale_pairs == []
    assert result.skipped_reasons == {"AUDUSD_PX_LAST": "blocked: No swap_2y series for AUD."}


def test_stale_pair_is_skipped_and_recorded_but_not_in_skipped_reasons():
    """skipped_reasons only carries blocked-pair reasons -- matching exactly what used to be
    logged inline (stale pairs were only ever counted, never logged as an individual line)."""
    as_of = pd.Timestamp("2024-06-01")
    availability = _g10_availability("EURUSD_PX_LAST", "EUR", "USD")
    stale_values = _fresh_wide_values(pd.Timestamp("2020-01-01"))  # far in the past -> stale
    api = _StubDataAPI(stale_values)

    result = build_silver_frame(api, "G10", _strategy_config("G10"), [availability], as_of=as_of)

    assert result.frame.empty
    assert result.fetched_pair_count == 0
    assert result.blocked_pairs == []
    assert len(result.stale_pairs) == 1
    assert result.stale_pairs[0].startswith("EURUSD_PX_LAST (")
    assert result.skipped_reasons == {}


def test_multiple_pairs_mix_of_blocked_fetched_and_stale():
    as_of = pd.Timestamp("2024-06-01")
    fresh_availability = _g10_availability("EURUSD_PX_LAST", "EUR", "USD")
    blocked_availability = _blocked_availability("AUDUSD_PX_LAST")
    values = _fresh_wide_values(as_of)
    api = _StubDataAPI(values)

    result = build_silver_frame(
        api,
        "G10",
        _strategy_config("G10"),
        [fresh_availability, blocked_availability],
        as_of=as_of,
    )

    assert result.pair_count == 2
    assert result.fetched_pair_count == 1
    assert result.blocked_pairs == ["AUDUSD_PX_LAST"]
    assert set(result.frame[SERIES_CODE_COLUMN]) == {"EURUSD_PX_LAST"}


def test_chn_flows_cutover_failure_is_reported_not_raised():
    """No metadata for the flow series -- resolve_flows_cutover fails; build_silver_frame
    records the error rather than raising (the asset decides whether/how to log it)."""
    as_of = pd.Timestamp("2024-06-01")
    availability = PairAvailability(
        series_code="USDCNH_PX_LAST",
        variant="CHN",
        base_currency="USD",
        quote_currency="CNH",
        resolved={
            ("base", "swap_2y"): "USD_SWAP",
            ("quote", "swap_2y"): "CNH_SWAP",
            ("base", "local_equity"): "USD_EQ",
            ("quote", "local_equity"): "CNH_EQ",
            ("quote", "cds_5y"): "CNH_CDS",
        },
    )
    api = _StubDataAPI(
        pd.DataFrame(), metadata=pd.DataFrame(columns=["series_code", "valid_to"])
    )  # no valid_to metadata at all

    result = build_silver_frame(
        api,
        "CHN",
        _strategy_config("CHN", DRIVER_NAMES + ("offshore_spread", "flows")),
        [availability],
        as_of=as_of,
    )

    assert result.chn_flows_cutover_error is not None
    assert "valid_to" in result.chn_flows_cutover_error


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


def test_em_pair_fetches_successfully():
    """Acceptance criterion: at least one G10 (see the other tests in this file), one EM,
    and one CHN pair all produce a well-formed, fetched output through build_silver_frame."""
    as_of = pd.Timestamp("2024-06-01")
    availability = _em_availability("USDZAR_PX_LAST", "USD", "ZAR", "ZAR")
    rng = np.random.default_rng(5)
    dates = pd.bdate_range(end=as_of, periods=90)
    columns = [
        "USDZAR_PX_LAST",
        "MXWO_PX_LAST",
        "BRENT_PX_LAST",
        "USD_SWAP",
        "ZAR_SWAP",
        "USD_EQ",
        "ZAR_EQ",
        "ZAR_CDS",
    ]
    values = pd.DataFrame(
        {col: 100 + np.cumsum(rng.normal(0, 0.5, 90)) for col in columns}, index=dates
    )
    api = _StubDataAPI(values)

    result = build_silver_frame(api, "EM", _strategy_config("EM"), [availability], as_of=as_of)

    assert result.fetched_pair_count == 1
    assert not result.frame.empty
    assert (result.frame[SERIES_CODE_COLUMN] == "USDZAR_PX_LAST").all()


def test_chn_pair_fetches_successfully_with_a_resolved_cutover():
    """The CHN success path (complementing the cutover-failure test above)."""
    as_of = pd.Timestamp("2024-06-01")
    availability = PairAvailability(
        series_code="USDCNH_PX_LAST",
        variant="CHN",
        base_currency="USD",
        quote_currency="CNH",
        resolved={
            ("base", "swap_2y"): "USD_SWAP",
            ("quote", "swap_2y"): "CNH_SWAP",
            ("base", "local_equity"): "USD_EQ",
            ("quote", "local_equity"): "CNH_EQ",
            ("quote", "cds_5y"): "CNH_CDS",
        },
    )
    rng = np.random.default_rng(7)
    dates = pd.bdate_range(end=as_of, periods=90)
    value_columns = [
        "USDCNH_PX_LAST",
        "MXWO_PX_LAST",
        "BRENT_PX_LAST",
        "USD_SWAP",
        "CNH_SWAP",
        "USD_EQ",
        "CNH_EQ",
        "CNH_CDS",
        "OFFSHORE_SPREAD_PX_LAST",
        "ONSHORE_SPREAD_PX_LAST",
        "SHANGHAI_BUY_FLOWS_PX_LAST",
        "SHENZHEN_BUY_FLOWS_PX_LAST",
        "SHANGHAI_SELL_FLOWS_PX_LAST",
        "SHENZHEN_SELL_FLOWS_PX_LAST",
        "SHANGHAI_FLOWS_TURNOVER_PX_LAST",
        "SHENZHEN_FLOWS_TURNOVER_PX_LAST",
    ]
    values = pd.DataFrame(
        {col: 100 + np.cumsum(rng.normal(0, 0.5, 90)) for col in value_columns}, index=dates
    )
    metadata = pd.DataFrame(
        {
            "series_code": [
                "SHANGHAI_BUY_FLOWS_PX_LAST",
                "SHENZHEN_BUY_FLOWS_PX_LAST",
                "SHANGHAI_SELL_FLOWS_PX_LAST",
                "SHENZHEN_SELL_FLOWS_PX_LAST",
            ],
            "valid_to": ["2024-08-16"] * 4,
        }
    )
    api = _StubDataAPI(values, metadata=metadata)

    result = build_silver_frame(
        api,
        "CHN",
        _strategy_config("CHN", DRIVER_NAMES + ("offshore_spread", "flows")),
        [availability],
        as_of=as_of,
    )

    assert result.chn_flows_cutover_error is None
    assert result.fetched_pair_count == 1
    assert not result.frame.empty
