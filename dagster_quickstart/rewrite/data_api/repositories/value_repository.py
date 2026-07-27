"""Business repository for time-series values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import ValueColumns
from dagster_quickstart.rewrite.data_api.errors import FrameValidationError, SeriesCodesRequiredError
from dagster_quickstart.rewrite.data_api.repositories.storage_repository import ValueStorageRepository

logger = structlog.get_logger(__name__)

REQUIRED_VALUE_COLUMNS = {ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE}


class ValueRepository:
    """
    Business repository for querying and persisting time-series values.

    This repository contains business logic only. All persistence is
    delegated to a ValueStorageRepository implementation.
    """

    def __init__(
        self,
        storage: ValueStorageRepository,
    ) -> None:
        self._storage = storage

    def get_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        order_by: str | None = ValueColumns.TIMESTAMP,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Retrieve value rows for one or more series.
        """

        self._validate_series_codes(series_codes)

        return self._storage.get_values(
            series_codes=series_codes,
            ticker_source=ticker_source,
            start=start,
            end=end,
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            version=version,
            as_of=as_of,
        )

    def get_last_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        latest_non_null: bool = True,
    ) -> pd.DataFrame:
        """
        Return the latest value for each requested series.
        """

        self._validate_series_codes(series_codes)

        return self._storage.get_last_values(
            series_codes=series_codes,
            ticker_source=ticker_source,
            latest_non_null=latest_non_null,
        )

    def save_values(
        self,
        frame: pd.DataFrame,
    ) -> None:
        """
        Persist value data.
        """

        if frame.empty:
            return

        self._validate_frame(frame)

        logger.info("value_repository_save", row_count=len(frame))
        self._storage.save_values(frame)

    def value_exists(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
    ) -> Mapping[str, bool]:
        self._validate_series_codes(series_codes)

        return self._storage.value_exists(
            series_codes,
            ticker_source=ticker_source,
        )

    def delete_values(
        self,
        filters: Mapping[str, object],
    ) -> None:
        """
        Delete values matching the supplied filters.
        """

        logger.info("value_repository_delete", filter_fields=sorted(filters.keys()))
        self._storage.delete_values(filters)

    def get_storage_path(self) -> str | None:
        """Return the common physical storage path backing the values table, if any."""

        return self._storage.get_storage_path()

    @staticmethod
    def _validate_series_codes(
        series_codes: Sequence[str],
    ) -> None:
        if not series_codes:
            logger.warning("value_repository_missing_series_codes")
            raise SeriesCodesRequiredError("At least one series code must be supplied.")

    @staticmethod
    def _validate_frame(
        frame: pd.DataFrame,
    ) -> None:
        missing = REQUIRED_VALUE_COLUMNS.difference(frame.columns)

        if missing:
            logger.warning("value_repository_missing_columns", missing=sorted(missing))
            raise FrameValidationError(f"Missing required columns: {sorted(missing)}")
