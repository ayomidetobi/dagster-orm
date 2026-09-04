"""Unit tests for dagster_quickstart.availability.storage: latest_snapshot, write_report/read_latest_report.

Uses a real in-memory DuckDB connection (via FakeRewriteDataAPIResource, same as
tests/test_steer_assets.py) since write_table()/read_table() go through a real
GenericTableRepository -- there's no meaningful fake for "append-only table storage" itself.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_quickstart.availability.storage import (
    latest_snapshot,
    read_latest_report,
    write_report,
)
from tests.test_steer_assets import FakeRewriteDataAPIResource


@pytest.fixture
def data_api():
    return FakeRewriteDataAPIResource(pd.DataFrame(), pd.DataFrame()).api


def test_latest_snapshot_returns_only_the_most_recent_as_of_rows():
    frame = pd.DataFrame(
        {
            "as_of": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-05", "2024-01-05"]),
            "series_code": ["A", "B", "A", "B"],
            "value": [1, 2, 3, 4],
        }
    )

    latest = latest_snapshot(frame)

    assert set(latest["as_of"].unique()) == {pd.Timestamp("2024-01-05")}
    assert set(latest["series_code"]) == {"A", "B"}
    assert len(latest) == 2


def test_latest_snapshot_empty_in_empty_out():
    assert latest_snapshot(pd.DataFrame(columns=["as_of", "value"])).empty


def test_read_latest_report_raises_when_nothing_stored(data_api):
    with pytest.raises(LookupError, match="run the fx_data_availability asset first"):
        read_latest_report(data_api, "G10")


def test_write_then_read_round_trips_the_report(data_api):
    report = pd.DataFrame(
        {"series_code": ["EURUSD_PX_LAST"], "variant": ["G10"], "blocked": [False]}
    )

    write_report(data_api, report)
    loaded = read_latest_report(data_api, "G10")

    assert loaded["series_code"].tolist() == ["EURUSD_PX_LAST"]
    assert "as_of" in loaded.columns


def test_second_write_on_a_later_date_appends_a_new_snapshot_read_returns_only_it():
    """Acceptance criterion 4: two reports written for the same variant on different dates --
    the read returns only the newer one's rows."""
    resource = FakeRewriteDataAPIResource(pd.DataFrame(), pd.DataFrame())
    api = resource.api

    older = pd.DataFrame({"series_code": ["OLD_PAIR"], "variant": ["G10"], "blocked": [True]})
    newer = pd.DataFrame({"series_code": ["NEW_PAIR"], "variant": ["G10"], "blocked": [False]})

    write_report(api, older, as_of=pd.Timestamp("2024-01-01"))
    write_report(api, newer, as_of=pd.Timestamp("2024-01-15"))

    loaded = read_latest_report(api, "G10")

    assert loaded["series_code"].tolist() == ["NEW_PAIR"]


def test_read_latest_report_only_returns_the_requested_variant(data_api):
    g10 = pd.DataFrame({"series_code": ["EURUSD_PX_LAST"], "variant": ["G10"], "blocked": [False]})
    em = pd.DataFrame({"series_code": ["USDZAR_PX_LAST"], "variant": ["EM"], "blocked": [False]})

    write_report(data_api, g10)
    write_report(data_api, em)

    loaded = read_latest_report(data_api, "EM")

    assert loaded["series_code"].tolist() == ["USDZAR_PX_LAST"]


def test_write_report_is_a_noop_for_an_empty_report(data_api):
    write_report(data_api, pd.DataFrame())

    with pytest.raises(LookupError):
        read_latest_report(data_api, "G10")


def test_read_latest_report_logs_the_age(data_api, capfd):
    """Acceptance criterion 5: the report age is logged at read (fit) time -- visible, never
    gated on (no threshold, no warning, no failure, just a log line)."""
    report = pd.DataFrame(
        {"series_code": ["EURUSD_PX_LAST"], "variant": ["G10"], "blocked": [False]}
    )
    as_of = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=23)
    write_report(data_api, report, as_of=as_of)

    read_latest_report(data_api, "G10")

    captured = capfd.readouterr()
    combined = captured.out + captured.err
    assert "using availability report from" in combined
    assert "23 days old" in combined
