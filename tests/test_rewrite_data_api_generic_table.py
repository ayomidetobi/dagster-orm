"""Unit tests for DataAPI.read_table()/.write_table() (rewrite.data_api.repositories.
generic_table_repository.GenericTableRepository) -- the generic, no-fixed-shape table
read/write STEER's silver/gold tables use instead of a second DuckLake attach + its own
rewrite-the-whole-table write.

write()'s whole point is that it never rewrites an existing row -- it widens the table (ALTER
TABLE ADD COLUMN) when a frame introduces a new column, then appends by explicit column name.
That's asserted directly here (test_write_never_issues_create_or_replace_table), not just
inferred from round-trip content, since a regression back to `CREATE OR REPLACE TABLE ... AS
SELECT * FROM existing UNION ALL BY NAME SELECT * FROM frame` would still pass every
content-only round-trip test in this file.

Both write orders (CHN's 7 columns first, then G10's 5; and the reverse) are tested explicitly,
since a suite that only ever writes the wider frame first wouldn't catch a regression that only
shows up the other way.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI
from dagster_quickstart.rewrite.data_api.factory import create_data_api

GOLD_SCHEMA = "gold"
TABLE = "steer_result_summary_test"


class _EmptyMetadataStorage:
    def get_metadata(self, **kwargs):
        return pd.DataFrame()

    def get_columns(self):
        return []

    def get_distinct_values(self, *args, **kwargs):
        return []

    def save_metadata(self, *args, **kwargs):
        raise NotImplementedError

    def refresh_metadata(self):
        pass


class _EmptyValueStorage:
    def get_values(self, *args, **kwargs):
        return pd.DataFrame()

    def get_last_values(self, *args, **kwargs):
        return pd.DataFrame()

    def value_exists(self, *args, **kwargs):
        return {}

    def save_values(self, *args, **kwargs):
        raise NotImplementedError

    def delete_values(self, *args, **kwargs):
        raise NotImplementedError

    def get_storage_path(self):
        return None


class _SQLRecordingConnection:
    """Wraps a real duckdb connection, recording every SQL string passed to .execute().

    Passed as `duckdb_connection=` to create_data_api() -- duckdb.DuckDBPyConnection's own
    instance attributes are read-only (a C extension type), so a real connection can't be
    monkeypatched directly; this proxy is used in its place instead, forwarding every call.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection
        self.statements: list[str] = []

    def execute(self, sql, parameters=None):
        self.statements.append(sql)
        if parameters is None:
            return self._connection.execute(sql)
        return self._connection.execute(sql, parameters)

    def register(self, name, frame):
        return self._connection.register(name, frame)

    def unregister(self, name):
        return self._connection.unregister(name)


def _data_api(connection) -> DataAPI:
    return create_data_api(
        duckdb_connection=connection,
        metadata_repository=_EmptyMetadataStorage(),
        value_repository=_EmptyValueStorage(),
    )


@pytest.fixture
def recording_connection() -> _SQLRecordingConnection:
    return _SQLRecordingConnection(duckdb.connect(":memory:"))


@pytest.fixture
def data_api(recording_connection) -> DataAPI:
    return _data_api(recording_connection)


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


def test_g10_then_chn_both_round_trip_with_all_columns_intact(data_api):
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())
    data_api.write_table(GOLD_SCHEMA, TABLE, _chn_row())

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    assert "coefficient_flows" in table.columns

    g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    chn_row = table[table["series_code"] == "USDCNH_PX_LAST"].iloc[0]
    assert g10_row["coefficient_interest_rate_differential"] == pytest.approx(0.5)
    assert pd.isna(g10_row["coefficient_offshore_spread"])  # G10 never had this driver
    assert chn_row["coefficient_offshore_spread"] == pytest.approx(-0.6)


def test_chn_then_g10_both_round_trip_with_all_columns_intact(data_api):
    """The opposite write order -- CHN (7 columns) creates the table first, then G10 (5) writes.
    A naive `CREATE TABLE IF NOT EXISTS ... LIMIT 0` from the FIRST write only ever fixes that
    write's own columns; if write() didn't also widen on every later write, this order would
    still have all 7 columns (CHN created them), but the reverse assertion -- that G10's row is
    present and unaffected -- still needs to hold, and a regression that only breaks the
    G10-widens-into-CHN direction wouldn't be caught by the other test alone."""
    data_api.write_table(GOLD_SCHEMA, TABLE, _chn_row())
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    assert "coefficient_flows" in table.columns

    g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    chn_row = table[table["series_code"] == "USDCNH_PX_LAST"].iloc[0]
    assert chn_row["coefficient_offshore_spread"] == pytest.approx(-0.6)
    assert pd.isna(g10_row["coefficient_offshore_spread"])
    assert g10_row["coefficient_interest_rate_differential"] == pytest.approx(0.5)


def test_write_narrower_frame_after_wider_one_does_not_drop_the_wider_columns(data_api):
    """A third write with even fewer columns than either prior write shouldn't narrow the table
    back down -- a column the table already has that a new frame lacks is NULL-padded, never
    dropped."""
    data_api.write_table(GOLD_SCHEMA, TABLE, _chn_row())
    narrow = pd.DataFrame([{"series_code": "USDZAR_PX_LAST", "universe": "EM"}])
    data_api.write_table(GOLD_SCHEMA, TABLE, narrow)

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame

    assert len(table) == 2
    assert "coefficient_offshore_spread" in table.columns
    em_row = table[table["series_code"] == "USDZAR_PX_LAST"].iloc[0]
    assert pd.isna(em_row["coefficient_offshore_spread"])


def test_write_never_issues_create_or_replace_table(data_api, recording_connection):
    """Acceptance criterion 2 -- the whole point of this change. Assert it directly: a
    regression back to `CREATE OR REPLACE TABLE ... UNION ALL BY NAME` would still pass every
    content-only round-trip test above."""
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())
    data_api.write_table(GOLD_SCHEMA, TABLE, _chn_row())
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())

    statements = [sql.upper() for sql in recording_connection.statements]
    assert not any("CREATE OR REPLACE" in sql for sql in statements)
    assert not any("UNION ALL BY NAME" in sql for sql in statements)
    assert any(sql.startswith("INSERT INTO") for sql in statements)


def test_write_never_reads_the_target_table_to_write_it(data_api, recording_connection):
    """Acceptance criterion 3, structurally rather than by timing: every write's SELECT source
    is the incoming frame's own (fixed-size) temp relation, never the accumulated target table
    -- so write cost never grows with how much is already in the table. The only queries that
    reference the qualified table by name are schema/existence probes (`... LIMIT 0`,
    information_schema), never a full-table read, and the table is a write TARGET (`INSERT INTO
    qualified (...)`) but never a read SOURCE (`FROM qualified` without `LIMIT 0`)."""
    for i in range(5):
        frame = pd.DataFrame([{"series_code": f"S{i}", "universe": "G10", "value": float(i)}])
        data_api.write_table(GOLD_SCHEMA, TABLE, frame)

    qualified = f'"{GOLD_SCHEMA}"."{TABLE}"'
    for sql in recording_connection.statements:
        if f"FROM {qualified}" in sql and "LIMIT 0" not in sql:
            pytest.fail(f"write_table read the target table as a data source: {sql!r}")


def test_repeated_writes_grow_the_table_by_exactly_one_row_each(data_api):
    """Acceptance criterion 3, content-based: 100 single-row writes must produce exactly 100
    rows -- O(n) rows written for O(n) writes, not some multiple of n from a rewrite."""
    for i in range(100):
        frame = pd.DataFrame([{"series_code": f"S{i}", "universe": "G10", "value": float(i)}])
        data_api.write_table(GOLD_SCHEMA, "many_writes", frame)

    table = data_api.read_table(GOLD_SCHEMA, "many_writes").frame
    assert len(table) == 100
    assert set(table["series_code"]) == {f"S{i}" for i in range(100)}


def test_read_table_on_a_nonexistent_table_returns_an_empty_result_not_an_exception(data_api):
    """Acceptance criterion 6."""
    result = data_api.read_table(GOLD_SCHEMA, "does_not_exist_yet")
    assert result.frame.empty

    filtered = data_api.read_table(GOLD_SCHEMA, "does_not_exist_yet", universe="G10")
    assert filtered.frame.empty


def test_read_table_result_chains_and_narrows_in_memory(data_api):
    """read_table() returns a MetadataResult -- chaining/filter_options() come for free."""
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())
    data_api.write_table(GOLD_SCHEMA, TABLE, _chn_row())

    results = data_api.read_table(GOLD_SCHEMA, TABLE, universe="G10")
    assert list(results.frame["series_code"]) == ["EURUSD_PX_LAST"]

    narrowed = data_api.read_table(GOLD_SCHEMA, TABLE).get_metadata(series_code="USDCNH_PX_LAST")
    assert list(narrowed.frame["series_code"]) == ["USDCNH_PX_LAST"]
    assert narrowed.filter_options("universe") == ["CHN"]


def test_all_null_object_column_does_not_lock_in_the_wrong_type_for_a_later_string_write(
    data_api,
):
    """Regression: a frame with an all-null role column (e.g. base_rate_3m, populated by G10
    but never by EM/CHN) used to let DuckDB infer INTEGER for that column at CREATE TABLE
    time -- a later write with real series-code strings in that column then failed with
    `_duckdb.ConversionException: Could not convert string ... to INT32`."""
    em_row = pd.DataFrame([{"series_code": "USDZAR_PX_LAST", "universe": "EM", "base_rate_3m": None}])
    g10_row = pd.DataFrame(
        [{"series_code": "EURUSD_PX_LAST", "universe": "G10", "base_rate_3m": "EUR3M_PX_LAST"}]
    )

    data_api.write_table(GOLD_SCHEMA, TABLE, em_row)
    data_api.write_table(GOLD_SCHEMA, TABLE, g10_row)

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame
    written_g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    written_em_row = table[table["series_code"] == "USDZAR_PX_LAST"].iloc[0]
    assert written_g10_row["base_rate_3m"] == "EUR3M_PX_LAST"
    assert pd.isna(written_em_row["base_rate_3m"])


def test_all_null_object_column_introduced_via_widening_does_not_lock_in_the_wrong_type(
    data_api,
):
    """Same regression via the widening path (ALTER TABLE ADD COLUMN) rather than CREATE
    TABLE -- the column is absent from the first write entirely, introduced all-null by the
    second write, and only given a real string value by a third."""
    data_api.write_table(
        GOLD_SCHEMA, TABLE, pd.DataFrame([{"series_code": "USDZAR_PX_LAST", "universe": "EM"}])
    )
    data_api.write_table(
        GOLD_SCHEMA,
        TABLE,
        pd.DataFrame([{"series_code": "USDCNH_PX_LAST", "universe": "CHN", "base_rate_3m": None}]),
    )
    data_api.write_table(
        GOLD_SCHEMA,
        TABLE,
        pd.DataFrame(
            [{"series_code": "EURUSD_PX_LAST", "universe": "G10", "base_rate_3m": "EUR3M_PX_LAST"}]
        ),
    )

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame
    written_g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    assert written_g10_row["base_rate_3m"] == "EUR3M_PX_LAST"


def test_all_null_numeric_column_is_left_alone(data_api):
    """A genuinely numeric all-null column (float64 dtype, NaN rather than None) must not be
    coerced to string -- only object-dtype all-null columns get the fix (see
    GenericTableRepository._coerce_all_null_object_columns)."""
    data_api.write_table(
        GOLD_SCHEMA,
        TABLE,
        pd.DataFrame([{"series_code": "USDZAR_PX_LAST", "universe": "EM", "score": float("nan")}]),
    )
    data_api.write_table(
        GOLD_SCHEMA,
        TABLE,
        pd.DataFrame([{"series_code": "EURUSD_PX_LAST", "universe": "G10", "score": 1.5}]),
    )

    table = data_api.read_table(GOLD_SCHEMA, TABLE).frame
    written_g10_row = table[table["series_code"] == "EURUSD_PX_LAST"].iloc[0]
    assert written_g10_row["score"] == pytest.approx(1.5)


def test_read_table_get_values_raises_a_clear_error_naming_the_table(data_api):
    """get_values()/get_last_values() aren't meaningful for an arbitrary table -- a clear error,
    not a deep, confusing failure inside MetadataResult."""
    data_api.write_table(GOLD_SCHEMA, TABLE, _g10_row())
    result = data_api.read_table(GOLD_SCHEMA, TABLE)

    with pytest.raises(NotImplementedError, match=TABLE):
        result.get_values()
    with pytest.raises(NotImplementedError, match=TABLE):
        result.get_last_values()
