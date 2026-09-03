"""Bloomberg TSS ("direct"/server-side) vendor clients.

BloombergClient (bloomberg.py) models Bloomberg's Desktop API -- used when a
local Bloomberg Terminal is available. This module models the alternative:
TSS (Bloomberg's Server API), used when there's no local terminal to talk to
-- see is_local() below for picking between the two.

TSS calls take an out_from_cache setting controlling whether TSS's own
server-side cache is consulted:
    "yes"    -- read from the cache
    "no"     -- force a live fetch, bypassing the cache
    "ignore" -- leave it unspecified (vendor default)

Each concrete client below fixes out_from_cache to one of those three, so a
caller picks the behavior by choosing which client/ticker_source to use
rather than passing it per-call. The fetch logic itself lives once, in
BloombergDirectClient, so it isn't duplicated three times.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pandas as pd

from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorClient
from dagster_quickstart.rewrite.data_api.vendors.demo_data import fetch_demo_values

OUT_FROM_CACHE_OPTIONS = ("yes", "no", "ignore")


class BloombergDirectClient(VendorClient):
    """Base Bloomberg TSS vendor client; out_from_cache is fixed by subclasses.

    Placeholder: returns random demo values, like every other vendor client
    here, until a real Bloomberg TSS integration is wired in.
    """

    out_from_cache: str = "ignore"

    def fetch(
        self,
        *,
        tickers: Mapping[str, str],
        field: str,
        start: datetime | None = None,
        end: datetime | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        return fetch_demo_values(
            f"BloombergDirect[out_from_cache={self.out_from_cache}]", tickers, start, end
        )


class BloombergDirectYesClient(BloombergDirectClient):
    """TSS client that reads from Bloomberg's server-side cache."""

    out_from_cache = "yes"


class BloombergDirectNoClient(BloombergDirectClient):
    """TSS client that forces a live fetch, bypassing Bloomberg's server-side cache."""

    out_from_cache = "no"


class BloombergDirectIgnoreClient(BloombergDirectClient):
    """TSS client that leaves the cache setting unspecified (vendor default)."""

    out_from_cache = "ignore"


def is_local() -> bool:
    """Whether this environment has a local Bloomberg Terminal available.

    Dummy placeholder, not a real environment check -- always returns True.
    Swap the body for a real one (e.g. checking whether blpapi/pyeqdr can
    connect, or an env var your deployment sets) when one is available.
    """
    return True


def resolve_bloomberg_client() -> VendorClient:
    """Pick the right Bloomberg vendor client for this environment.

    Local (is_local() -> True): BloombergClient, Desktop API.
    Server (is_local() -> False): BloombergDirectIgnoreClient, TSS/Server API.
    """
    if is_local():
        from dagster_quickstart.rewrite.data_api.vendors.bloomberg import BloombergClient

        return BloombergClient()

    return BloombergDirectIgnoreClient()
