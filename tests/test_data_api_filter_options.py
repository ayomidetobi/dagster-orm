import pandas as pd
import pytest

from dagster_quickstart.orm.data_api import DataAPI
from dagster_quickstart.orm.exceptions import InvalidFilterFieldError


@pytest.fixture
def api() -> DataAPI:
    api = object.__new__(DataAPI)
    api.load_lookup_table_from_s3 = lambda: pd.DataFrame(
        {
            "asset_class": ["Equity", "Commodity", "Equity", None],
            "country": ["United States", "Germany", "", "Japan"],
        }
    )
    return api


def test_filter_options_single_field_returns_list(api: DataAPI) -> None:
    assert api.filter_options(fields="asset_class") == ["Equity", "Commodity"]


def test_filter_options_multiple_fields_returns_mapping(api: DataAPI) -> None:
    assert api.filter_options(fields=["asset_class", "country"]) == {
        "asset_class": ["Equity", "Commodity"],
        "country": ["United States", "Germany", "Japan"],
    }


def test_query_options_alias_uses_same_behavior(api: DataAPI) -> None:
    assert api.query_options(for_="asset_class") == ["Equity", "Commodity"]


def test_query_options_alias_supports_as_dataframe(api: DataAPI) -> None:
    result = api.query_options(for_="country", as_dataframe=True)

    assert list(result.columns) == ["field", "value"]
    assert result.to_dict("records") == [
        {"field": "country", "value": "United States"},
        {"field": "country", "value": "Germany"},
        {"field": "country", "value": "Japan"},
    ]


def test_filter_options_rejects_unknown_field(api: DataAPI) -> None:
    with pytest.raises(InvalidFilterFieldError):
        api.filter_options(fields="missing_field")
