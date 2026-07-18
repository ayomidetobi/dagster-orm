"""Tests for direct vendor-source fetch alignment."""

import pandas as pd

from dagster_quickstart.orm.direct_source_fetch import (
    align_wide_to_expected_series,
    reshape_direct_source_df,
)
from dagster_quickstart.orm.schema import ValueColumns


def test_align_wide_pads_missing_series_with_nan() -> None:
    tickers = {
        "SX0012_PX_LAST": "T12",
        "SX0014_PX_LAST": "T14",
        "SX0099_PX_LAST": "T99",
    }
    raw = pd.DataFrame(
        {"T12": [1.0, 2.0], "T14": [3.0, 4.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
    )

    out = align_wide_to_expected_series(raw, tickers)

    assert list(out.columns) == list(tickers.keys())
    assert out["SX0099_PX_LAST"].isna().all()
    assert list(out["SX0012_PX_LAST"]) == [1.0, 2.0]


def test_align_wide_flattens_multiindex_columns() -> None:
    tickers = {"A": "tA", "B": "tB"}
    raw = pd.DataFrame(
        [[1.0], [2.0]],
        index=pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
        columns=pd.MultiIndex.from_tuples([("tA", "PX_LAST")]),
    )

    out = align_wide_to_expected_series(raw, tickers)

    assert list(out.columns) == ["A", "B"]
    assert out["B"].isna().all()
    assert list(out["A"]) == [1.0, 2.0]


def test_reshape_keeps_nan_values_in_long_form() -> None:
    wide = pd.DataFrame(
        {"S1": [1.0, None], "S2": [None, None]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
    )
    wide.index.name = ValueColumns.TIMESTAMP

    long = reshape_direct_source_df(wide)

    assert len(long) == 4
    assert long[ValueColumns.VALUE].isna().sum() == 3
