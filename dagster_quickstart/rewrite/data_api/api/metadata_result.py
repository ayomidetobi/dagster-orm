"""Wrapper around a metadata query result that can fetch its own values.

Lets a caller go straight from a metadata filter to values for the matched
series, without manually pulling series_code back out and re-passing it:

    context = data_api.get_metadata(asset_class=["Equity", "Commodity"])
    values = context.get_values(start=start, ticker_source="BBG")
    latest = context.get_last_values(ticker_source="BBG")

Otherwise behaves like the underlying DataFrame (indexing, iteration, len,
repr, attribute access) via delegation, so existing pd.DataFrame-shaped
callers of get_metadata() keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from rewrite.data_api.columns import MetadataColumns


class MetadataResult:
    """A metadata DataFrame that can fetch values for its own series codes."""

    def __init__(
        self,
        frame: pd.DataFrame,
        *,
        fetch_values: Callable[..., pd.DataFrame],
        fetch_last_values: Callable[..., pd.DataFrame],
    ) -> None:
        self._frame = frame
        self._fetch_values = fetch_values
        self._fetch_last_values = fetch_last_values

    @property
    def frame(self) -> pd.DataFrame:
        """The underlying metadata DataFrame."""
        return self._frame

    @property
    def info(self) -> pd.DataFrame:
        """The underlying metadata DataFrame (alias for `.frame`).

        Defined as a real property so it returns the metadata rows rather
        than falling through __getattr__ to pandas' DataFrame.info(), which
        prints a dtype/memory summary instead of the data.
        """
        return self._frame

    @property
    def series_codes(self) -> list[str]:
        """The distinct, non-empty series codes present in this result."""
        if self._frame.empty or MetadataColumns.SERIES_CODE not in self._frame.columns:
            return []
        codes = self._frame[MetadataColumns.SERIES_CODE].dropna().astype(str).str.strip()
        return [code for code in dict.fromkeys(codes.tolist()) if code]

    def get_values(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch values for this result's series codes.

        Keyword arguments are forwarded to DataAPI.get_values() (e.g.
        ticker_source, start, end, order_by, limit, out_of_cache).
        """
        return self._fetch_values(self.series_codes, **kwargs)

    def get_last_values(self, *, ticker_source: str | None = None) -> pd.DataFrame:
        """Fetch the latest value row for each of this result's series."""
        return self._fetch_last_values(self.series_codes, ticker_source=ticker_source)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._frame, name)

    def __getitem__(self, key: Any) -> Any:
        return self._frame[key]

    def __len__(self) -> int:
        return len(self._frame)

    def __iter__(self):
        return iter(self._frame)

    def __repr__(self) -> str:
        return repr(self._frame)
