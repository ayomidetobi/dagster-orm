"""Direct (out-of-cache) vendor value fetch orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import structlog

from rewrite.data_api.columns import MetadataColumns, ValueColumns
from rewrite.data_api.services.metadata_service import MetadataService
from rewrite.data_api.services.vendor_service import VendorService
from rewrite.data_api.vendors.direct_fetch import (
    get_derived_direct_values,
    get_direct_values,
    sort_and_limit,
)

logger = structlog.get_logger(__name__)


class DirectFetchService:
    """Fetch value rows straight from the vendor, bypassing DuckLake.

    Splits requested series into derived (computed from parent series) vs.
    primary before dispatching, mirroring the metadata/metadata_derived split
    used by the old orm/ system's out_of_cache queries. Derived-series
    support is opt-in: pass derived_metadata_service=None to treat every
    requested series as primary.
    """

    def __init__(
        self,
        metadata_service: MetadataService,
        vendor_service: VendorService,
        derived_metadata_service: MetadataService | None = None,
    ) -> None:
        self._metadata = metadata_service
        self._vendors = vendor_service
        self._derived_metadata = derived_metadata_service

    def get_values(
        self,
        series_codes: Sequence[str],
        ticker_source: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Return live value rows for the requested series."""

        if not series_codes:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        logger.info(
            "direct_fetch_started",
            ticker_source=ticker_source,
            series_count=len(series_codes),
        )

        derived_codes, primary_codes = self._split_derived_and_primary(series_codes)

        frames: list[pd.DataFrame] = []

        if primary_codes:
            frames.append(
                get_direct_values(
                    self._metadata,
                    self._vendors,
                    primary_codes,
                    ticker_source,
                    start=start,
                    end=end,
                )
            )

        if derived_codes:
            frames.append(
                get_derived_direct_values(
                    self._metadata,
                    self._derived_metadata,
                    self._vendors,
                    derived_codes,
                    ticker_source,
                    start=start,
                    end=end,
                )
            )

        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return pd.DataFrame(columns=["series_code", "timestamp", "value"])

        combined = pd.concat(non_empty, ignore_index=True)
        return sort_and_limit(combined, order_by=order_by, limit=limit)

    def _split_derived_and_primary(
        self,
        series_codes: Sequence[str],
    ) -> tuple[list[str], list[str]]:
        if self._derived_metadata is None:
            return [], list(series_codes)

        derived_df = self._derived_metadata.list_metadata(
            {MetadataColumns.SERIES_CODE: list(series_codes)}
        )
        if derived_df.empty:
            return [], list(series_codes)

        derived_set = set(derived_df[MetadataColumns.SERIES_CODE].astype(str).str.strip())
        derived = [code for code in series_codes if str(code).strip() in derived_set]
        primary = [code for code in series_codes if str(code).strip() not in derived_set]
        return derived, primary
