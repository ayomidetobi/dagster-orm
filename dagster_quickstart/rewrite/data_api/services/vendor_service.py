"""Vendor orchestration logic."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.errors import UnsupportedVendorError

logger = structlog.get_logger(__name__)


class VendorClient(Protocol):
    """Contract for vendor-specific fetchers."""

    def fetch(self, **kwargs: object) -> pd.DataFrame:
        """Fetch a normalized vendor frame."""


class VendorService:
    """Dispatch vendor reads to the correct client."""

    def __init__(self, clients: Mapping[str, VendorClient]):
        """Initialize the vendor service."""
        self._clients = dict(clients)

    def fetch(self, vendor: str, **kwargs: object) -> pd.DataFrame:
        """Fetch a normalized DataFrame from a vendor client."""
        try:
            client = self._clients[vendor]
        except KeyError as exc:
            logger.warning("unsupported_vendor", vendor=vendor, known_vendors=sorted(self._clients))
            raise UnsupportedVendorError(f"Unsupported vendor: {vendor!r}") from exc
        logger.info("vendor_fetch_started", vendor=vendor)
        return client.fetch(**kwargs)
