import pandas as pd

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.queryset import QuerySet
from dagster_quickstart.orm.schema import MetadataColumns, TableNames


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
    pass


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
