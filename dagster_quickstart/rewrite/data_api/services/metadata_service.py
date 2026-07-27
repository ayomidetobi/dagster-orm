"""Metadata business logic."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import MetadataColumns
from dagster_quickstart.rewrite.data_api.quality import (
    DEFAULT_NULL_CHECK_COLUMNS,
    MetadataQualityReport,
    build_quality_report,
    frames_equal,
)
from dagster_quickstart.rewrite.data_api.repositories.metadata_repository import MetadataRepository
from dagster_quickstart.rewrite.data_api.validation import strip_whitespace, validate_metadata_frame

logger = structlog.get_logger(__name__)

#: How many snapshots back to search for this table's real previous state.
#: ducklake_snapshots() is catalog-wide, so most of a table's own snapshot
#: history can be writes to *other* tables that leave this one unchanged --
#: this caps how far back get_quality_report() walks looking for one that
#: actually differs, rather than scanning the entire catalog history.
MAX_SNAPSHOT_LOOKBACK = 50


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

    def get_quality_report(
        self,
        *,
        null_check_columns: Sequence[str] = DEFAULT_NULL_CHECK_COLUMNS,
    ) -> MetadataQualityReport:
        """Report data-quality signals for the metadata catalog.

        Compares the current (latest) metadata state against this table's
        own real previous state -- found by walking backward through
        DuckLake's snapshot history -- no external reference file involved
        -- to flag newly introduced columns/column values, alongside
        duplicate series_code rows and null-value counts in the current
        state.

        ducklake_snapshots() is catalog-wide (every table's writes, not just
        this one), so the immediately-preceding snapshot_id often leaves
        this table's content unchanged (e.g. a write to a different table).
        Walking backward and comparing content (not just snapshot_id order)
        finds the snapshot that actually changed this table -- capped at
        MAX_SNAPSHOT_LOOKBACK to bound how much catalog history gets scanned.

        null_check_columns controls which columns get flagged for null
        values -- defaults to DEFAULT_NULL_CHECK_COLUMNS
        (series_code/series_name); pass more (e.g. asset_class) as needed.

        Reads straight from the repository (bypassing schema validation) so
        the report can still be built even if the very defects it looks for
        (e.g. a null series_code already sitting in the catalog) would
        otherwise fail validation.
        """
        current = self._repository.get_metadata({})
        snapshots = self._repository.list_snapshots()

        current_version: int | None = None
        baseline_version: int | None = None
        baseline: pd.DataFrame | None = None

        if not snapshots.empty:
            snapshot_ids = [int(value) for value in snapshots["snapshot_id"].tolist()]
            current_version = snapshot_ids[-1]
            candidate_versions = snapshot_ids[:-1][-MAX_SNAPSHOT_LOOKBACK:]

            for version in reversed(candidate_versions):
                try:
                    candidate = self._repository.get_metadata({}, version=version)
                except Exception:
                    logger.debug("quality_report_baseline_predates_table", version=version)
                    break
                if not frames_equal(candidate, current):
                    baseline_version = version
                    baseline = candidate
                    break

        return build_quality_report(
            current=current,
            baseline=baseline,
            current_version=current_version,
            baseline_version=baseline_version,
            null_check_columns=null_check_columns,
        )
