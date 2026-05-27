"""Tests for out-of-cache derived series computation."""

import pandas as pd

import dagster_quickstart.orm.queryset as queryset_module
from dagster_quickstart.orm.derived_calc import compute_derived_series
from dagster_quickstart.orm.derived_fetch import get_derived_out_of_cache_values
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, TickerSource, ValueColumns


def test_compute_derived_spread_from_parent_pivot() -> None:
    parent_pivot = pd.DataFrame(
        {
            "SX0012_PX_LAST": [10.0, 11.0, 12.0],
            "SX0014_PX_LAST": [8.0, 9.0, 10.0],
        },
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True),
    )
    out = compute_derived_series(
        "SPREAD",
        parent_pivot,
        ["SX0012_PX_LAST", "SX0014_PX_LAST"],
    )
    assert list(out.values) == [2.0, 2.0, 2.0]


def test_get_derived_out_of_cache_values_spread() -> None:
    derived_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["DER_EQ_SPR_SX1214"],
            MetadataColumns.PARENT_SERIES_CODE: ["SX0012_PX_LAST|SX0014_PX_LAST"],
            MetadataColumns.CALC_TYPE: ["SPREAD"],
        }
    )
    primary_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["SX0012_PX_LAST", "SX0014_PX_LAST"],
            MetadataColumns.BBG_TICKER: ["T12", "T14"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )

    parent_long = pd.DataFrame(
        {
            ValueColumns.SERIES_CODE: ["SX0012_PX_LAST"] * 2 + ["SX0014_PX_LAST"] * 2,
            ValueColumns.TIMESTAMP: pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
                utc=True,
            ),
            ValueColumns.VALUE: [10.0, 11.0, 8.0, 9.0],
        }
    )

    original_direct = queryset_module.get_direct_source_values

    def fake_direct(load_metadata_rows, series_codes, tickersource, params):
        assert set(series_codes) == {"SX0012_PX_LAST", "SX0014_PX_LAST"}
        return parent_long

    queryset_module.get_direct_source_values = fake_direct
    try:
        out = get_derived_out_of_cache_values(
            load_primary_metadata_rows=lambda _f: primary_df,
            load_derived_dependency_rows=lambda _codes: derived_df,
            derived_series_codes=["DER_EQ_SPR_SX1214"],
            tickersource=TickerSource.BLOOMBERG,
            params=None,
        )
    finally:
        queryset_module.get_direct_source_values = original_direct

    assert not out.empty
    assert set(out[ValueColumns.SERIES_CODE]) == {"DER_EQ_SPR_SX1214"}
    assert list(out[ValueColumns.VALUE]) == [2.0, 2.0]


def test_queryset_value_out_of_cache_derived_uses_derived_fetch() -> None:
    derived_meta = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["DER_EQ_SPR_SX1214"],
            MetadataColumns.PARENT_SERIES_CODE: ["SX0012_PX_LAST|SX0014_PX_LAST"],
            MetadataColumns.CALC_TYPE: ["SPREAD"],
        }
    )

    class DerivedAwareMetadataRepository:
        def filter(self, filters=None, control_type=TableNames.METADATA, exclude=False, allow_empty=None):
            codes = (filters or {}).get(MetadataColumns.SERIES_CODE, [])
            if control_type == TableNames.METADATA_DERIVED:
                df = derived_meta.copy()
                if codes:
                    df = df[df[MetadataColumns.SERIES_CODE].isin(codes)]
                return df.reset_index(drop=True)
            return pd.DataFrame().reset_index(drop=True)

    class NoOpValueRepository:
        def get_batch_series_data(self, *args, **kwargs):
            return pd.DataFrame()

    qs = QuerySet(
        metadata_repository=DerivedAwareMetadataRepository(),
        value_repository=NoOpValueRepository(),
        series_codes=["DER_EQ_SPR_SX1214"],
        out_of_cache=True,
        control_table=TableNames.METADATA_DERIVED,
    )

    expected = pd.DataFrame(
        {
            ValueColumns.SERIES_CODE: ["DER_EQ_SPR_SX1214"],
            ValueColumns.TIMESTAMP: pd.to_datetime(["2024-01-01"], utc=True),
            ValueColumns.VALUE: [2.0],
        }
    )

    original = queryset_module.get_derived_out_of_cache_values

    def fake_derived(**kwargs):
        return expected

    queryset_module.get_derived_out_of_cache_values = fake_derived
    try:
        wide = qs.value(tickersource=TickerSource.BLOOMBERG)
    finally:
        queryset_module.get_derived_out_of_cache_values = original

    assert "DER_EQ_SPR_SX1214" in wide.columns
    assert wide.loc[pd.Timestamp("2024-01-01", tz="UTC"), "DER_EQ_SPR_SX1214"] == 2.0
