from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar

from dagster_quickstart.rewrite.data_api import DataAPI
from dagster_quickstart.rewrite.data_api.api.queryset import QuerySet


class DatasetBase(ABC):
    """Base class for predefined semantic datasets."""

    _api: ClassVar[DataAPI | None] = None
    _FILTERS: ClassVar[dict[str, Any]] = {}

    @classmethod
    def configure(cls, api: DataAPI) -> None:
        """Configure the shared DataAPI instance."""
        cls._api = api

    @classmethod
    def api(cls) -> DataAPI:
        if cls._api is None:
            raise RuntimeError(
                "DatasetBase has not been configured. "
                "Call DatasetBase.configure(DataAPI(...)) before using datasets."
            )
        return cls._api

    @classmethod
    def build_queryset(cls) -> QuerySet:
        """
        Build the base QuerySet for this dataset.

        Most datasets only need to define `_FILTERS`. More complex datasets
        can override this method.
        """
        return cls.api().query().filter(**cls._FILTERS)

    def __init__(self, *, out_of_cache: bool | None = None, ticker_source: str | None = None):
        self._out_of_cache = out_of_cache
        self._ticker_source = ticker_source
        self.query = self.build_queryset()

    def _resolve_out_of_cache(self, out_of_cache: bool | None) -> bool:
        return self._out_of_cache if out_of_cache is None else out_of_cache

    def _resolve_ticker_source(self, ticker_source: str | None) -> str | None:
        return self._ticker_source if ticker_source is None else ticker_source

    @property
    def info(self):
        """Return the metadata rows matched by this dataset's filters."""
        return self.query.metadata()

    def get_values(self, *, out_of_cache: bool | None = None, ticker_source: str | None = None):
        """Return value rows for this dataset's series.

        out_of_cache/ticker_source default to whatever this dataset was
        constructed with; pass either to override for this call only.
        out_of_cache=True bypasses DuckLake and fetches live from the
        vendor named by ticker_source (see QuerySet.live()) -- required in
        that case, since (unlike the old orm system) nothing here infers a
        vendor from metadata; omitting it raises TickerSourceRequiredError.

        A ticker_source passed here while this call resolves to cached
        (out_of_cache=False) raises -- QuerySet has no way to filter a
        cached read by vendor yet. A ticker_source that's only a
        constructor default (not passed to this call) is silently unused
        for a cached call instead, since it just means "the vendor to use
        when going live", which doesn't apply here.
        """
        if self._resolve_out_of_cache(out_of_cache):
            return self.query.live(self._resolve_ticker_source(ticker_source)).value()
        if ticker_source is not None:
            raise ValueError(
                "ticker_source only applies to a live (out_of_cache=True) fetch -- "
                "QuerySet has no way to filter a cached read by vendor yet."
            )
        return self.query.cached().value()

    def last_values(self, *, out_of_cache: bool | None = None):
        """Return the latest value row for each of this dataset's series.

        "Latest value, live" isn't a vendor operation -- last_value() always
        reads from DuckLake regardless of out_of_cache, matching
        QuerySet.last_value()/DataAPI.get_last_values(). Requesting
        out_of_cache=True here raises rather than silently doing a cached
        read anyway, so that assumption doesn't go unnoticed.
        """
        if self._resolve_out_of_cache(out_of_cache):
            raise ValueError(
                "last_values() always reads from DuckLake -- there's no live "
                "equivalent for 'the latest value'. Use "
                "get_values(out_of_cache=True) and take the last row instead."
            )
        return self.query.last_value()

    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying QuerySet.

        Guards against infinite recursion if `query` itself isn't set yet
        (e.g. a subclass overrides __init__ without calling super()) --
        without this, looking up the missing `query` attribute would
        re-enter __getattr__("query"), which looks up `self.query` again,
        forever.
        """
        try:
            query = self.__dict__["query"]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {name!r} "
                "(query isn't set -- did __init__ run?)"
            ) from None
        return getattr(query, name)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(filters={self._FILTERS}, "
            f"out_of_cache={self._out_of_cache}, "
            f"ticker_source={self._ticker_source})"
        )
