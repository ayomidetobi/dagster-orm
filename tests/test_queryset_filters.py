import pandas as pd

import dagster_quickstart.orm.queryset as queryset_module
from dagster_quickstart.orm.data_api import DataAPI, FX
from dagster_quickstart.orm.exceptions import ValueQueryParameterError
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, TickerSource, ValueColumns


class FakeMetadataRepository:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe
        self.calls = []

    def filter(self, filters=None, control_type=TableNames.METADATA, exclude=False, allow_empty=None):
        self.calls.append(
            {
                "filters": filters,
                "control_type": control_type,
                "exclude": exclude,
                "allow_empty": allow_empty,
            }
        )
        df = self.dataframe.copy()
        if filters:
            for field, values in filters.items():
                if values:
                    df = df[df[field].isin(values)]
        return df.reset_index(drop=True)


class FakeValueRepository:
    def __init__(self):
        self.batch_df = pd.DataFrame()
        self.last_df = pd.DataFrame()
        self.last_calls = []
        self.batch_calls = []

    def get_batch_series_data(
        self,
        series_codes,
        tickersource=TickerSource.BLOOMBERG,
        start=None,
        end=None,
        order_by=None,
        limit=None,
    ):
        self.batch_calls.append(
            {
                "series_codes": series_codes,
                "tickersource": tickersource,
                "start": start,
                "end": end,
                "order_by": order_by,
                "limit": limit,
            }
        )
        return self.batch_df.copy()

    def get_last_values(
        self,
        series_codes,
        tickersource=TickerSource.BLOOMBERG,
        latest_non_null=True,
    ):
        self.last_calls.append(
            {
                "series_codes": series_codes,
                "tickersource": tickersource,
                "latest_non_null": latest_non_null,
            }
        )
        result = self.last_df.copy()
        if latest_non_null:
            result = result.dropna(subset=[ValueColumns.VALUE]).reset_index(drop=True)
        return result


def make_queryset() -> QuerySet:
    metadata_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2", "S3"],
            MetadataColumns.ASSET_CLASS: ["FX", "FX", "Equity"],
            MetadataColumns.COUNTRY: ["USA", "UK", "USA"],
            MetadataColumns.CURRENCY: ["USD", "GBP", "USD"],
            MetadataColumns.DEFAULT_SOURCE: [
                TickerSource.BLOOMBERG.value,
                TickerSource.BLOOMBERG.value,
                TickerSource.HAWKEYE.value,
            ],
        }
    )
    return QuerySet(
        metadata_repository=FakeMetadataRepository(metadata_df),
        value_repository=FakeValueRepository(),
        metadata_filters={MetadataColumns.ASSET_CLASS: ["FX"]},
        control_table=TableNames.METADATA_WILDCARD,
    )


def make_value_queryset() -> QuerySet:
    queryset = make_queryset()
    queryset._resolved_series_codes = ["S1", "S2"]
    queryset._value_repository.batch_df = pd.DataFrame(
        {
            ValueColumns.TIMESTAMP: pd.to_datetime(
                ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                utc=True,
            ),
            ValueColumns.SERIES_CODE: ["S1", "S2", "S1", "S2", "S1", "S2"],
            ValueColumns.VALUE: [1.0, 2.0, None, None, None, 3.0],
        }
    )
    queryset._value_repository.last_df = pd.DataFrame(
        {
            ValueColumns.TIMESTAMP: pd.to_datetime(
                ["2024-01-02", "2024-01-03"],
                utc=True,
            ),
            ValueColumns.SERIES_CODE: ["S1", "S2"],
            ValueColumns.VALUE: [5.0, None],
        }
    )
    return queryset


def test_repr_preserves_chained_include_filters() -> None:
    qs = make_queryset().filter(country="USA")

    repr_text = repr(qs)
    assert "include_filters={'asset_class': ['FX'], 'country': ['USA']}" in repr_text
    assert "exclude_filters={}" in repr_text


def test_filter_does_not_resolve_series_codes_eagerly() -> None:
    qs = make_queryset()
    repo = qs._metadata_repository

    chained = qs.filter(country="USA")

    assert repo.calls == []
    assert chained._series_codes is None


def test_filter_chaining_intersects_same_field_values() -> None:
    qs = QuerySet(
        metadata_repository=FakeMetadataRepository(
            pd.DataFrame(
                {
                    MetadataColumns.SERIES_CODE: ["S1", "S2", "S3"],
                    MetadataColumns.ASSET_CLASS: ["FX", "Rates", "Equity"],
                    MetadataColumns.COUNTRY: ["USA", "UK", "USA"],
                    MetadataColumns.CURRENCY: ["USD", "GBP", "USD"],
                    MetadataColumns.DEFAULT_SOURCE: [
                        TickerSource.BLOOMBERG.value,
                        TickerSource.BLOOMBERG.value,
                        TickerSource.HAWKEYE.value,
                    ],
                }
            )
        ),
        value_repository=FakeValueRepository(),
        metadata_filters={MetadataColumns.ASSET_CLASS: ["FX", "Rates"]},
        control_table=TableNames.METADATA_WILDCARD,
    )

    chained = qs.filter(asset_class="FX")

    assert chained._include_filters == {MetadataColumns.ASSET_CLASS: ["FX"]}
    assert list(chained.info()[MetadataColumns.ASSET_CLASS].unique()) == ["FX"]


def test_get_excluding_creates_exclude_filters() -> None:
    api = object.__new__(DataAPI)
    api._metadata_repository = FakeMetadataRepository(pd.DataFrame())
    api._value_repository = FakeValueRepository()
    api._validation_repository = None
    api._out_of_cache = False

    qs = api.get_excluding(country="USA")

    assert qs._include_filters == {}
    assert qs._exclude_filters == {"country": ["USA"]}


def test_filter_exclude_chains_with_include_filters() -> None:
    qs = make_queryset().filter_exclude(country="USA").filter(currency="GBP")

    assert qs._include_filters == {
        MetadataColumns.ASSET_CLASS: ["FX"],
        MetadataColumns.CURRENCY: ["GBP"],
    }
    assert qs._exclude_filters == {MetadataColumns.COUNTRY: ["USA"]}


def test_data_api_default_out_of_cache_flows_to_queryset() -> None:
    api = object.__new__(DataAPI)
    api._metadata_repository = FakeMetadataRepository(pd.DataFrame())
    api._value_repository = FakeValueRepository()
    api._validation_repository = None
    api._out_of_cache = True

    qs = api.get(asset_class="FX")

    assert qs._out_of_cache is True


def test_queryset_value_explicit_override_wins_over_default() -> None:
    queryset = make_value_queryset()
    queryset._out_of_cache = False

    queryset.value(out_of_cache=False)

    assert len(queryset._value_repository.batch_calls) == 1


def test_fx_get_includes_asset_class_fx() -> None:
    fx = object.__new__(FX)
    fx._metadata_repository = FakeMetadataRepository(pd.DataFrame())
    fx._value_repository = FakeValueRepository()
    fx._validation_repository = None
    fx._out_of_cache = False

    qs = fx.get()

    assert qs._include_filters == {MetadataColumns.ASSET_CLASS: ["FX"]}


def test_fx_get_combines_asset_class_and_country() -> None:
    fx = object.__new__(FX)
    fx._metadata_repository = FakeMetadataRepository(pd.DataFrame())
    fx._value_repository = FakeValueRepository()
    fx._validation_repository = None
    fx._out_of_cache = False

    qs = fx.get(country="USA")

    assert qs._include_filters == {
        MetadataColumns.ASSET_CLASS: ["FX"],
        MetadataColumns.COUNTRY: ["USA"],
    }


def test_fx_get_rejects_asset_class_override() -> None:
    fx = object.__new__(FX)

    try:
        fx.get(asset_class="Equity")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert str(exc) == "`asset_class` is predefined by FX and cannot be overridden."


def test_fx_default_out_of_cache_is_used_by_queryset_value() -> None:
    queryset = make_value_queryset()
    queryset._out_of_cache = True

    original_direct_fetch = queryset_module.get_direct_source_values

    def fake_direct_fetch(*args, **kwargs):
        return queryset._value_repository.batch_df.copy()

    queryset_module.get_direct_source_values = fake_direct_fetch
    try:
        queryset.value()
    finally:
        queryset_module.get_direct_source_values = original_direct_fetch

    assert len(queryset._value_repository.batch_calls) == 0


def test_fx_value_override_disables_default_out_of_cache() -> None:
    queryset = make_value_queryset()
    queryset._out_of_cache = True

    queryset.value(out_of_cache=False)

    assert len(queryset._value_repository.batch_calls) == 1


def test_chained_filters_preserve_out_of_cache_default() -> None:
    queryset = make_queryset()
    queryset._out_of_cache = True

    chained = queryset.filter(country="USA").filter_exclude(currency="USD")

    assert chained._out_of_cache is True


def test_fx_get_excluding_means_fx_universe_excluding_usa() -> None:
    fx = object.__new__(FX)
    fx._metadata_repository = FakeMetadataRepository(pd.DataFrame())
    fx._value_repository = FakeValueRepository()
    fx._validation_repository = None
    fx._out_of_cache = False

    qs = fx.get_excluding(country="USA")

    assert qs._include_filters == {MetadataColumns.ASSET_CLASS: ["FX"]}
    assert qs._exclude_filters == {MetadataColumns.COUNTRY: ["USA"]}


def test_queryset_filter_options_returns_context_specific_values() -> None:
    values = make_queryset().filter_options("country")

    assert values == ["USA", "UK"]


def test_queryset_filter_options_multiple_fields_returns_dict() -> None:
    values = make_queryset().filter_options(["country", "currency"])

    assert values == {
        "country": ["USA", "UK"],
        "currency": ["USD", "GBP"],
    }


def test_queryset_filter_options_as_dataframe_returns_field_value_rows() -> None:
    values = make_queryset().filter_options(["country", "currency"], as_dataframe=True)

    assert list(values.columns) == ["field", "value"]
    assert values.to_dict("records") == [
        {"field": "country", "value": "USA"},
        {"field": "country", "value": "UK"},
        {"field": "currency", "value": "USD"},
        {"field": "currency", "value": "GBP"},
    ]


def test_value_business_days_true_drops_all_nan_rows() -> None:
    values = make_value_queryset().value(business_days=True)

    assert list(values.index.strftime("%Y-%m-%d")) == ["2024-01-01", "2024-01-03"]


def test_value_business_days_true_keeps_partial_nan_rows() -> None:
    values = make_value_queryset().value(business_days=True)

    assert pd.Timestamp("2024-01-03", tz="UTC") in values.index
    assert pd.isna(values.loc[pd.Timestamp("2024-01-03", tz="UTC"), "S1"])
    assert values.loc[pd.Timestamp("2024-01-03", tz="UTC"), "S2"] == 3.0


def test_value_business_days_false_keeps_all_rows() -> None:
    values = make_value_queryset().value(business_days=False)

    assert list(values.index.strftime("%Y-%m-%d")) == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_get_values_business_days_true_behaves_the_same() -> None:
    values = make_value_queryset().get_values(business_days=True)

    assert list(values.index.strftime("%Y-%m-%d")) == ["2024-01-01", "2024-01-03"]


def test_value_without_ticker_source_groups_by_metadata_default_source() -> None:
    metadata_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.DEFAULT_SOURCE: [
                TickerSource.BLOOMBERG.value,
                TickerSource.HAWKEYE.value,
            ],
        }
    )
    value_repository = FakeValueRepository()

    def get_batch_series_data(series_codes, tickersource=TickerSource.BLOOMBERG, **kwargs):
        value_repository.batch_calls.append(
            {
                "series_codes": series_codes,
                "tickersource": tickersource,
                **kwargs,
            }
        )
        value_by_source = {
            TickerSource.BLOOMBERG: 1.0,
            TickerSource.HAWKEYE: 2.0,
        }
        return pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-01-01"], utc=True),
                ValueColumns.SERIES_CODE: [series_codes[0]],
                ValueColumns.VALUE: [value_by_source[tickersource]],
            }
        )

    value_repository.get_batch_series_data = get_batch_series_data
    queryset = QuerySet(
        metadata_repository=FakeMetadataRepository(metadata_df),
        value_repository=value_repository,
        metadata_filters=None,
        series_codes=["S1", "S2"],
        control_table=TableNames.METADATA_WILDCARD,
    )

    values = queryset.value(params=ValueQueryParams(start="2024-01-01", limit=5))

    assert value_repository.batch_calls == [
        {
            "series_codes": ["S1"],
            "tickersource": TickerSource.BLOOMBERG,
            "start": "2024-01-01",
            "end": None,
            "order_by": None,
            "limit": 5,
        },
        {
            "series_codes": ["S2"],
            "tickersource": TickerSource.HAWKEYE,
            "start": "2024-01-01",
            "end": None,
            "order_by": None,
            "limit": 5,
        },
    ]
    assert values.loc[pd.Timestamp("2024-01-01", tz="UTC"), "S1"] == 1.0
    assert values.loc[pd.Timestamp("2024-01-01", tz="UTC"), "S2"] == 2.0


def test_get_values_explicit_ticker_source_overrides_metadata_default_source() -> None:
    queryset = make_value_queryset()

    queryset.get_values(ticker_source=TickerSource.HAWKEYE)

    assert queryset._value_repository.batch_calls[-1]["tickersource"] == TickerSource.HAWKEYE


def test_get_values_without_ticker_source_raises_for_missing_default_source() -> None:
    metadata_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.DEFAULT_SOURCE: [""],
        }
    )
    queryset = QuerySet(
        metadata_repository=FakeMetadataRepository(metadata_df),
        value_repository=FakeValueRepository(),
        metadata_filters=None,
        series_codes=["S1"],
        control_table=TableNames.METADATA_WILDCARD,
    )

    try:
        queryset.get_values()
        assert False, "Expected ValueQueryParameterError"
    except ValueQueryParameterError as exc:
        assert "missing metadata default_source" in str(exc)


def test_get_last_values_business_days_true_ignores_nan_latest_values() -> None:
    queryset = make_value_queryset()
    values = queryset.get_last_values(business_days=True)

    assert queryset._value_repository.last_calls[-1]["latest_non_null"] is True
    assert list(values.columns) == ["S1"]
    assert list(values.index.strftime("%Y-%m-%d")) == ["2024-01-02"]
    assert values.loc[pd.Timestamp("2024-01-02", tz="UTC"), "S1"] == 5.0


def test_get_last_values_business_days_false_keeps_old_behavior() -> None:
    queryset = make_value_queryset()
    values = queryset.get_last_values(business_days=False)

    assert queryset._value_repository.last_calls[-1]["latest_non_null"] is False
    assert pd.Timestamp("2024-01-03", tz="UTC") in values.index
    assert pd.isna(values.loc[pd.Timestamp("2024-01-03", tz="UTC"), "S2"])
