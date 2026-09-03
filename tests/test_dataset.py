"""Tests for the dataset/ layer (DatasetBase, FXMajor) against a real QuerySet.

Fakes sit at the lowest (storage) layer only -- MetadataRepository,
ValueRepository, MetadataService, ValueService, and QuerySet are all real,
so these tests exercise the actual filter/pivot/validation logic, not a
mocked-out version of it.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_quickstart.rewrite.data_api.api.queryset import QuerySet
from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase
from dagster_quickstart.rewrite.data_api.dataset.fx import FXMajor
from dagster_quickstart.rewrite.data_api.errors import TickerSourceRequiredError
from dagster_quickstart.rewrite.data_api.repositories.metadata_repository import MetadataRepository
from dagster_quickstart.rewrite.data_api.repositories.value_repository import ValueRepository
from dagster_quickstart.rewrite.data_api.services.metadata_service import MetadataService
from dagster_quickstart.rewrite.data_api.services.value_service import ValueService

METADATA = pd.DataFrame(
    {
        "series_code": ["EURUSD", "GBPUSD", "USDJPY"],
        "asset_class": ["Currency", "Currency", "Currency"],
        "sub_asset_class": ["Forex Spot", "Forex Spot", "Forex Spot"],
        "market_development": ["G10", "G10", "G10"],
        "currency": ["EUR", "GBP", "JPY"],
    }
)

VALUES = pd.DataFrame(
    {
        "series_code": ["EURUSD", "EURUSD", "GBPUSD", "GBPUSD", "USDJPY"],
        "timestamp": pd.to_datetime(
            ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02", "2024-01-02"]
        ),
        "value": [1.1, 1.11, 1.25, 1.26, 148.0],
    }
)


class FakeMetadataStorage:
    """Minimal in-memory stand-in for MetadataStorageRepository."""

    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def get_columns(self) -> list[str]:
        return list(self._frame.columns)

    def _filtered(self, filters, *, exclude=False):
        frame = self._frame
        if filters:
            mask = pd.Series(True, index=frame.index)
            for field, values in filters.items():
                mask &= frame[field].isin(values)
            frame = frame[~mask] if exclude else frame[mask]
        return frame

    def get_metadata(self, filters=None, *, exclude=False, version=None, as_of=None):
        return self._filtered(filters, exclude=exclude).reset_index(drop=True)

    def get_distinct_values(self, column, *, filters=None, exclude=False):
        return sorted(self._filtered(filters, exclude=exclude)[column].dropna().unique().tolist())

    def save_metadata(self, frame, *, fresh=False):
        raise NotImplementedError

    def refresh_metadata(self):
        pass


class FakeValueStorage:
    """Minimal in-memory stand-in for ValueStorageRepository."""

    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def get_values(self, series_codes, **kwargs):
        frame = self._frame[self._frame["series_code"].isin(series_codes)]
        return frame.reset_index(drop=True)

    def get_last_values(
        self, series_codes, *, ticker_source=None, latest_non_null=True, version=None, as_of=None
    ):
        frame = self._frame[self._frame["series_code"].isin(series_codes)]
        return frame.sort_values("timestamp").groupby("series_code", as_index=False).tail(1)

    def value_exists(self, series_codes, *, ticker_source=None):
        existing = set(self._frame["series_code"].unique())
        return {code: code in existing for code in series_codes}

    def save_values(self, frame):
        raise NotImplementedError

    def delete_values(self, filters):
        raise NotImplementedError

    def get_storage_path(self):
        return None


class FakeDirectFetch:
    """Stands in for DirectFetchService -- just records calls and returns canned rows."""

    def __init__(self, frame: pd.DataFrame):
        self.frame = frame
        self.calls: list[dict] = []

    def get_values(self, series_codes, ticker_source, **kwargs):
        self.calls.append(
            {"series_codes": list(series_codes), "ticker_source": ticker_source, **kwargs}
        )
        return self.frame[self.frame["series_code"].isin(series_codes)].reset_index(drop=True)


class FakeDataAPI:
    """Just enough of DataAPI's surface for DatasetBase.build_queryset()."""

    def __init__(self, metadata_df=METADATA, values_df=VALUES, direct_fetch=None):
        self.metadata_service = MetadataService(
            MetadataRepository(FakeMetadataStorage(metadata_df))
        )
        self.value_service = ValueService(ValueRepository(FakeValueStorage(values_df)))
        self.direct_fetch = direct_fetch

    def query(self) -> QuerySet:
        return QuerySet(
            self.metadata_service,
            self.value_service,
            direct_fetch_service=self.direct_fetch,
        )


@pytest.fixture(autouse=True)
def configured_api():
    """Point every dataset class at a fresh FakeDataAPI for the duration of one test."""
    api = FakeDataAPI()
    DatasetBase.configure(api)
    yield api
    DatasetBase._api = None


def test_fx_major_filters_narrow_to_g10_major_fx_spot():
    fx = FXMajor()

    assert sorted(fx.info["series_code"]) == ["EURUSD", "GBPUSD", "USDJPY"]


def test_get_values_default_out_of_cache_reads_cache():
    fx = FXMajor(out_of_cache=False)

    values = fx.get_values()

    assert sorted(values.columns) == ["EURUSD", "GBPUSD", "USDJPY"]
    assert values.loc[pd.Timestamp("2024-01-01"), "EURUSD"] == 1.1


def test_get_values_per_call_override_wins_over_constructor_default():
    direct_fetch = FakeDirectFetch(VALUES)
    api = FakeDataAPI(direct_fetch=direct_fetch)
    DatasetBase.configure(api)
    fx = FXMajor(out_of_cache=True, ticker_source="bloomberg")

    fx.get_values(out_of_cache=False)

    assert direct_fetch.calls == []  # the live path was never touched


def test_get_values_out_of_cache_true_requires_ticker_source():
    fx = FXMajor(out_of_cache=True)

    with pytest.raises(TickerSourceRequiredError):
        fx.get_values()


def test_get_values_out_of_cache_true_uses_direct_fetch_with_ticker_source():
    direct_fetch = FakeDirectFetch(VALUES)
    api = FakeDataAPI(direct_fetch=direct_fetch)
    DatasetBase.configure(api)
    fx = FXMajor(out_of_cache=True, ticker_source="bloomberg")

    fx.get_values()

    assert direct_fetch.calls[0]["ticker_source"] == "bloomberg"
    assert sorted(direct_fetch.calls[0]["series_codes"]) == ["EURUSD", "GBPUSD", "USDJPY"]


def test_get_values_ticker_source_without_out_of_cache_raises():
    fx = FXMajor()

    with pytest.raises(ValueError, match="only applies to a live"):
        fx.get_values(ticker_source="bloomberg")


def test_last_values_returns_latest_row_per_series():
    fx = FXMajor()

    latest = fx.last_values()

    assert latest.loc[pd.Timestamp("2024-01-02"), "EURUSD"] == 1.11
    assert latest.loc[pd.Timestamp("2024-01-02"), "USDJPY"] == 148.0


def test_last_values_rejects_out_of_cache_true():
    fx = FXMajor(out_of_cache=True, ticker_source="bloomberg")

    with pytest.raises(ValueError, match="always reads from DuckLake"):
        fx.last_values()


def test_last_values_out_of_cache_false_override_is_fine_even_with_live_default():
    fx = FXMajor(out_of_cache=True, ticker_source="bloomberg")

    latest = fx.last_values(out_of_cache=False)

    assert not latest.empty


def test_matrix_relabels_series_code_columns_to_currency():
    fx = FXMajor()

    matrix = fx.matrix()

    assert sorted(matrix.columns) == ["EUR", "GBP", "JPY"]
    assert matrix.loc[pd.Timestamp("2024-01-01"), "EUR"] == 1.1


def test_matrix_raises_on_duplicate_currency():
    metadata = METADATA.copy()
    metadata.loc[metadata["series_code"] == "USDJPY", "currency"] = "EUR"  # force a collision
    api = FakeDataAPI(metadata_df=metadata)
    DatasetBase.configure(api)
    fx = FXMajor()

    with pytest.raises(ValueError, match="duplicates"):
        fx.matrix()


def test_repr_includes_filters_and_overrides():
    fx = FXMajor(out_of_cache=True, ticker_source="bloomberg")

    text = repr(fx)

    assert "FXMajor" in text
    assert "out_of_cache=True" in text
    assert "ticker_source=bloomberg" in text


def test_unknown_attribute_delegates_to_queryset():
    fx = FXMajor()

    options = fx.filter_options("currency")

    assert sorted(options) == ["EUR", "GBP", "JPY"]


def test_getattr_guard_raises_attributeerror_not_recursionerror_when_query_missing():
    fx = object.__new__(FXMajor)  # __init__ never ran -- no `query` attribute set

    with pytest.raises(AttributeError, match="query isn't set"):
        fx.anything
