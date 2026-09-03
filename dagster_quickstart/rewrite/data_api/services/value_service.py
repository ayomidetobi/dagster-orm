"""Value business logic."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import ValueColumns
from dagster_quickstart.rewrite.data_api.repositories.value_repository import ValueRepository
from dagster_quickstart.rewrite.data_api.validation import coerce_numeric_value, validate_value_frame

logger = structlog.get_logger(__name__)


class ValueService:
    """Coordinate value reads and writes."""

    def __init__(self, repository: ValueRepository):
        """Initialize the service."""
        self._repository = repository

    def read_values(
        self,
        series_codes: Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        ticker_source: str | None = None,
        order_by: str | None = ValueColumns.TIMESTAMP,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Read value rows for a series set."""
        logger.info(
            "value_service_query",
            series_count=len(series_codes),
            start=start,
            end=end,
            ticker_source=ticker_source,
        )
        frame = self._repository.get_values(
            series_codes,
            ticker_source=ticker_source,
            start=start,
            end=end,
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            version=version,
            as_of=as_of,
        )
        return validate_value_frame(frame)

    def read_last_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
    ) -> pd.DataFrame:
        """Read the latest value rows for a series set."""
        logger.info(
            "value_service_last_query",
            series_count=len(series_codes),
            ticker_source=ticker_source,
        )
        frame = self._repository.get_last_values(series_codes, ticker_source=ticker_source)
        return validate_value_frame(frame)

    def write_values(self, frame: pd.DataFrame) -> None:
        """Write a normalized value frame to storage.

        Non-numeric values (a real vendor's "NOT FOUND"/"N/A"/etc. for a
        missing data point) are coerced to NaN before validation -- see
        coerce_numeric_value -- so one bad point doesn't crash the whole
        batch write with a DuckDB ConversionException.
        """
        logger.info("value_service_write", row_count=len(frame))
        validated = validate_value_frame(coerce_numeric_value(frame))
        self._repository.save_values(validated)

    def value_exists(self, series_codes: Sequence[str]) -> Mapping[str, bool]:
        """Check whether value rows exist for the requested series."""
        return self._repository.value_exists(series_codes)

    def get_storage_path(self) -> str | None:
        """Return the common physical storage path backing the values table, if any."""
        return self._repository.get_storage_path()
