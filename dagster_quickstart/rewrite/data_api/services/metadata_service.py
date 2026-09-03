"""Metadata business logic."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import MetadataColumns
from dagster_quickstart.rewrite.data_api.repositories.metadata_repository import MetadataRepository
from dagster_quickstart.rewrite.data_api.validation import strip_whitespace, validate_metadata_frame

logger = structlog.get_logger(__name__)


class MetadataService:
    """Coordinate metadata queries and catalog writes.

    Backs both the primary metadata table and the metadata_derived table
    (same class, different repository/table_name) -- pass a different
    validator (e.g. validate_derived_metadata_frame) for the latter.
    """

    def __init__(
        self,
        repository: MetadataRepository,
        validator: Callable[[pd.DataFrame], pd.DataFrame] = validate_metadata_frame,
    ):
        """Initialize the service."""
        self._repository = repository
        self._validate = validator

    def list_metadata(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
        strict: bool = False,
    ) -> pd.DataFrame:
        """Return matching metadata rows.

        strict controls how unrecognized filter values are handled -- see
        MetadataRepository.get_metadata().
        """
        logger.info(
            "metadata_service_query",
            exclude=exclude,
            strict=strict,
            filter_fields=sorted(filters.keys()) if filters else [],
        )
        frame = self._repository.get_metadata(
            filters,
            exclude=exclude,
            version=version,
            as_of=as_of,
            strict=strict,
        )
        return self._validate(frame)

    def list_columns(self) -> list[str]:
        """Return the available metadata column names (valid filter keys)."""
        return self._repository.get_columns()

    def filter_options(
        self,
        fields: str | Sequence[str] | None = None,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
        strict: bool = False,
        as_dataframe: bool = False,
    ) -> list[str] | dict[str, list[str]] | pd.DataFrame:
        """Return available metadata filter values.

        fields=None returns options for every column. Pass filters to narrow
        the options to a subset (e.g. currency values within
        asset_class=Equity). strict controls how unrecognized values in that
        narrowing filter are handled -- see MetadataRepository.get_metadata().
        """
        logger.info(
            "metadata_service_filter_options",
            fields=fields,
            strict=strict,
            filter_fields=sorted(filters.keys()) if filters else [],
        )
        options = self._repository.get_filter_options(
            fields, filters=filters, exclude=exclude, strict=strict
        )

        if not as_dataframe:
            return options

        options_by_field = {fields: options} if isinstance(options, list) else options
        rows = [
            {"field": field, "value": value}
            for field, values in options_by_field.items()
            for value in values
        ]
        return pd.DataFrame(rows, columns=["field", "value"])

    def resolve_series_codes(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        strict: bool = False,
    ) -> list[str]:
        """Resolve matching series codes from metadata."""
        frame = self.list_metadata(filters, exclude=exclude, strict=strict)
        if frame.empty or MetadataColumns.SERIES_CODE not in frame.columns:
            return []
        series_codes = frame[MetadataColumns.SERIES_CODE].dropna().astype(str).str.strip()
        return [code for code in series_codes.tolist() if code]

    def import_metadata(self, frame: pd.DataFrame, *, fresh: bool = False) -> pd.DataFrame:
        """Persist a normalized metadata frame, returning the validated rows.

        fresh=True replaces any existing rows for this frame's series_codes
        instead of appending alongside them -- see
        MetadataRepository.save_metadata().
        """
        logger.info("metadata_service_import", row_count=len(frame), fresh=fresh)
        validated = self._validate(strip_whitespace(frame))
        self._repository.save_metadata(validated, fresh=fresh)
        return validated

    def refresh_metadata(self) -> None:
        """Refresh repository-backed metadata state."""
        self._repository.refresh()
