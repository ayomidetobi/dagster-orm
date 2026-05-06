import pandas as pd

from dagster_quickstart.orm.data_api import DataAPI
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
