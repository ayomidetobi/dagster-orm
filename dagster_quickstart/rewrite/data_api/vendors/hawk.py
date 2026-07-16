"""Hawk vendor client."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pandas as pd

from rewrite.data_api.services.vendor_service import VendorClient
from rewrite.data_api.vendors.demo_data import fetch_demo_values


class HawkClient(VendorClient):
    """Fetch Hawk history.

    Placeholder: returns random values within a range for the requested
    series until a real Hawk integration is wired in.
    """

    def fetch(
        self,
        *,
        tickers: Mapping[str, str],
        start: datetime | None = None,
        end: datetime | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        return fetch_demo_values("Hawk", tickers, start, end)
