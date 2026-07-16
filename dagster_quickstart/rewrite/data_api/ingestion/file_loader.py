"""CSV/Excel ingestion into the DuckLake-backed data lake."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import structlog

from resources.duckdb_datacacher import SQL, DuckDBDataCacher
from rewrite.data_api.errors import UnsupportedFileTypeError
from rewrite.data_api.services.metadata_service import MetadataService
from rewrite.data_api.services.value_service import ValueService

logger = structlog.get_logger(__name__)

SUPPORTED_CSV_EXTENSIONS = (".csv",)
SUPPORTED_EXCEL_EXTENSIONS = (".xlsx", ".xls")


def read_tabular_file(path: str | Path) -> pd.DataFrame:
    """Read a CSV or Excel file into a DataFrame based on its extension."""

    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in SUPPORTED_CSV_EXTENSIONS:
        return pd.read_csv(path)

    if suffix in SUPPORTED_EXCEL_EXTENSIONS:
        return pd.read_excel(path)

    logger.warning("unsupported_file_type", path=str(path), suffix=suffix)
    raise UnsupportedFileTypeError(f"Unsupported file extension: {suffix!r}")


class FileIngestionService:
    """Land CSV/Excel files in the lake.

    Each file gets a raw, unvalidated Parquet copy archived to S3 (for
    lineage/audit) before the normalized frame is loaded into the DuckLake
    catalog through the metadata/value services.
    """

    def __init__(
        self,
        metadata_service: MetadataService,
        value_service: ValueService,
        cacher: DuckDBDataCacher,
    ) -> None:
        self._metadata = metadata_service
        self._values = value_service
        self._cacher = cacher

    def ingest_metadata_file(
        self,
        path: str | Path,
        *,
        raw_prefix: str = "raw/metadata",
        service: MetadataService | None = None,
    ) -> pd.DataFrame:
        """Load a metadata CSV/Excel file into a DuckLake metadata table.

        Defaults to the primary metadata table; pass a different
        MetadataService (e.g. one backed by "metadata_derived") to load
        series_dependencies-style files with the same loader.
        """

        logger.info("metadata_file_ingestion_started", path=str(path))
        frame = read_tabular_file(path)
        self._archive(frame, path, raw_prefix)
        (service or self._metadata).import_metadata(frame)
        logger.info("metadata_file_ingestion_completed", path=str(path), row_count=len(frame))
        return frame

    def ingest_value_file(
        self,
        path: str | Path,
        *,
        raw_prefix: str = "raw/values",
    ) -> pd.DataFrame:
        """Load a value CSV/Excel file into the DuckLake values table."""

        logger.info("value_file_ingestion_started", path=str(path))
        frame = read_tabular_file(path)
        self._archive(frame, path, raw_prefix)
        self._values.write_values(frame)
        logger.info("value_file_ingestion_completed", path=str(path), row_count=len(frame))
        return frame

    def _archive(self, frame: pd.DataFrame, path: str | Path, raw_prefix: str) -> None:
        """Write a raw, pre-validation Parquet copy of the file to S3."""

        stem = Path(path).stem
        ingested_at = datetime.now().strftime("%Y%m%dT%H%M%S")
        file_path = f"{raw_prefix}/{stem}/ingested_at={ingested_at}/data.parquet"

        logger.info("file_ingestion_archive", file_path=file_path, row_count=len(frame))
        self._cacher.save(SQL("SELECT * FROM $frame", frame=frame), file_path=file_path)
