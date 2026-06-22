"""Tests for wide monthly partition merge and numeric sanitization."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from dagster_quickstart.orm.storage.wide_partition import (
    merge_wide_monthly_partition,
    sanitize_wide_numeric_columns,
)

UTC = timezone.utc


def test_sanitize_wide_numeric_columns_maps_not_found_to_nan():
    df = pd.DataFrame(
        {"SERIES_A": ["NOT FOUND", "1.5", "N/A"], "SERIES_B": [1.0, 2.0, 3.0]},
        index=pd.DatetimeIndex(
            [
                datetime(2026, 6, 20, tzinfo=UTC),
                datetime(2026, 6, 21, tzinfo=UTC),
                datetime(2026, 6, 22, tzinfo=UTC),
            ],
            name="timestamp",
        ),
    )

    out = sanitize_wide_numeric_columns(df)

    assert out["SERIES_A"].dtype == np.float64
    assert np.isnan(out.loc[df.index[0], "SERIES_A"])
    assert out.loc[df.index[1], "SERIES_A"] == 1.5
    assert np.isnan(out.loc[df.index[2], "SERIES_A"])


def test_merge_wide_monthly_partition_coerces_incoming_strings():
    existing = pd.DataFrame(
        {"SERIES_A": [10.0]},
        index=pd.DatetimeIndex([datetime(2026, 6, 20, tzinfo=UTC)], name="timestamp"),
    )
    incoming = pd.DataFrame(
        {"SERIES_A": ["NOT FOUND"], "SERIES_B": ["2.5"]},
        index=pd.DatetimeIndex([datetime(2026, 6, 21, tzinfo=UTC)], name="timestamp"),
    )

    merged = merge_wide_monthly_partition(existing, incoming, strip_date_range=None)

    assert merged["SERIES_A"].dtype == np.float64
    assert merged.loc[existing.index[0], "SERIES_A"] == 10.0
    assert np.isnan(merged.loc[incoming.index[0], "SERIES_A"])
    assert merged.loc[incoming.index[0], "SERIES_B"] == 2.5
