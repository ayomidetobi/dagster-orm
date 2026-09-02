"""Unit tests for steer.storage.SteerCatalog -- in particular write()'s column-widening behavior.

write() uses `CREATE OR REPLACE TABLE ... AS SELECT * FROM existing UNION ALL BY NAME SELECT *
FROM frame` for every write after the first, which re-derives the table's column set as the union
of whatever's already there and whatever's being written -- so ensure_table's initial `CREATE
TABLE IF NOT EXISTS ... LIMIT 0` (which only sees the FIRST frame's columns) never gets to fix a
narrower schema permanently. Both write orders are tested explicitly, since a suite that only ever
writes G10 (5 drivers) before CHN (7) wouldn't catch a regression that only shows up the other way.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from dagster_quickstart.steer.storage import GOLD_SCHEMA, SteerCatalog

TABLE = "steer_result_summary_test"


@pytest.fixture
def catalog() -> SteerCatalog:
    steer_catalog = SteerCatalog(duckdb.connect(":memory:"))
    steer_catalog.ensure_schemas()
    return steer_catalog


def _g10_row() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_code": "EURUSD_PX_LAST",
                "universe": "G10",
                "coefficient_interest_rate_differential": 0.5,
                "coefficient_yield_curve_or_cds": -0.3,
                "coefficient_local_equity": 0.2,
                "coefficient_global_equity": 0.1,
                "coefficient_commodity": 0.05,
            }
        ]
    )


def _chn_row() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_code": "USDCNH_PX_LAST",
                "universe": "CHN",
                "coefficient_interest_rate_differential": 0.4,
                "coefficient_yield_curve_or_cds": 120.0,
                "coefficient_local_equity": 0.1,
                "coefficient_global_equity": 0.2,
                "coefficient_commodity": 0.02,
                "coefficient_offshore_spread": -0.6,
                "coefficient_flows": 0.0,
            }
        ]
    )


def test_g10_then_chn_both_round_trip_with_all_columns_intact(catalog):
    catalog.write(GOLD_SCHEMA, TABLE, _g10_row())
    catalog.write(GOLD_SCHEMA, TABLE, _chn_row())

    table = catalog.read(GOLD_SCHEMA, TABLE)

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    assert "coefficient_flows" in table.columns

    g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    chn_row = table[table["series_code"] == "USDCNH_PX_LAST"].iloc[0]
    assert g10_row["coefficient_interest_rate_differential"] == pytest.approx(0.5)
    assert pd.isna(g10_row["coefficient_offshore_spread"])  # G10 never had this driver
    assert chn_row["coefficient_offshore_spread"] == pytest.approx(-0.6)


def test_chn_then_g10_both_round_trip_with_all_columns_intact(catalog):
    """The opposite write order -- CHN (7 columns) creates the table first, then G10 (5) writes.
    A naive `CREATE TABLE IF NOT EXISTS ... LIMIT 0` from the FIRST write only ever fixes that
    write's own columns; if write() didn't also widen on every later write, this order would still
    have all 7 columns (CHN created them), but the reverse assertion -- that G10's row is present
    and unaffected -- still needs to hold, and a regression that only breaks the G10-widens-into-CHN
    direction wouldn't be caught by test_g10_then_chn_both_round_trip_with_all_columns_intact alone."""
    catalog.write(GOLD_SCHEMA, TABLE, _chn_row())
    catalog.write(GOLD_SCHEMA, TABLE, _g10_row())

    table = catalog.read(GOLD_SCHEMA, TABLE)

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    assert "coefficient_flows" in table.columns

    g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    chn_row = table[table["series_code"] == "USDCNH_PX_LAST"].iloc[0]
    assert chn_row["coefficient_offshore_spread"] == pytest.approx(-0.6)
    assert pd.isna(g10_row["coefficient_offshore_spread"])
    assert g10_row["coefficient_interest_rate_differential"] == pytest.approx(0.5)


def test_write_narrower_frame_after_wider_one_does_not_drop_the_wider_columns(catalog):
    """A third write with even fewer columns than either prior write shouldn't narrow the table
    back down -- UNION ALL BY NAME only adds NULLs for columns a given frame lacks, never removes
    a column the table already has."""
    catalog.write(GOLD_SCHEMA, TABLE, _chn_row())
    narrow = pd.DataFrame([{"series_code": "USDZAR_PX_LAST", "universe": "EM"}])
    catalog.write(GOLD_SCHEMA, TABLE, narrow)

    table = catalog.read(GOLD_SCHEMA, TABLE)

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    em_row = table[table["series_code"] == "USDZAR_PX_LAST"].iloc[0]
    assert pd.isna(em_row["coefficient_offshore_spread"])
