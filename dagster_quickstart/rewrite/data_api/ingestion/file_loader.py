"""CSV/Excel ingestion into the DuckLake-backed data lake."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import structlog

from rewrite.data_api.errors import UnsupportedFileTypeError
from rewrite.data_api.services.metadata_service import MetadataService
from rewrite.data_api.services.value_service import ValueService

logger = structlog.get_logger(__name__)

SUPPORTED_CSV_EXTENSIONS = (".csv",)
SUPPORTED_EXCEL_EXTENSIONS = (".xlsx", ".xls")


def read_tabular_file(path: str | Path, *, sheet: str | int | None = None) -> pd.DataFrame:
    """Read a CSV or Excel file into a DataFrame based on its extension.

    sheet selects a sheet by name or index for Excel files; ignored for CSV.
    """

    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in SUPPORTED_CSV_EXTENSIONS:
        return pd.read_csv(path)

    if suffix in SUPPORTED_EXCEL_EXTENSIONS:
        return pd.read_excel(path, sheet_name=sheet if sheet is not None else 0)

    logger.warning("unsupported_file_type", path=str(path), suffix=suffix)
    raise UnsupportedFileTypeError(f"Unsupported file extension: {suffix!r}")


class FileIngestionService:
    """Land CSV/Excel files in the DuckLake-backed data lake.

    Reads the file, then writes it straight through the metadata/value
    services. DuckLake alone decides where the data physically lives (its
    own S3 layout, partitioning, and snapshot-per-write history) -- there is
    no separate ingestion-owned path or raw copy outside its catalog.
    """

    def __init__(
        self,
        metadata_service: MetadataService,
        value_service: ValueService,
    ) -> None:
        self._metadata = metadata_service
        self._values = value_service

    def ingest_metadata_file(
        self,
        path: str | Path,
        *,
        sheet: str | int | None = None,
        fresh: bool = False,
        service: MetadataService | None = None,
    ) -> pd.DataFrame:
        """Load a metadata CSV/Excel file into a DuckLake metadata table.

        sheet selects a sheet by name or index for Excel files; ignored for
        CSV. Defaults to the primary metadata table; pass a different
        MetadataService (e.g. one backed by "metadata_derived") to load
        series_dependencies-style files with the same loader. fresh=True
        replaces any existing rows for this file's series_codes instead of
        appending alongside them -- see MetadataService.import_metadata().
        """

        logger.info("metadata_file_ingestion_started", path=str(path))
        frame = read_tabular_file(path, sheet=sheet)
        validated = (service or self._metadata).import_metadata(frame, fresh=fresh)
        logger.info("metadata_file_ingestion_completed", path=str(path), row_count=len(frame))
        return validated

    def ingest_value_file(
        self,
        path: str | Path,
        *,
        sheet: str | int | None = None,
    ) -> pd.DataFrame:
        """Load a value CSV/Excel file into the DuckLake values table."""

        logger.info("value_file_ingestion_started", path=str(path))
        frame = read_tabular_file(path, sheet=sheet)
        self._values.write_values(frame)
        logger.info("value_file_ingestion_completed", path=str(path), row_count=len(frame))
        return frame
