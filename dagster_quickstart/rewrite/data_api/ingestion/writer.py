"""Value ingestion writer."""

from __future__ import annotations

import pandas as pd
import structlog

from rewrite.data_api.services.value_service import ValueService

logger = structlog.get_logger(__name__)


class IngestionWriter:
    """Persist normalized vendor frames into DuckLake."""

    def __init__(self, service: ValueService):
        """Initialize the writer."""
        self._service = service

    def write(self, frame: pd.DataFrame) -> None:
        """Write a normalized frame to storage."""
        logger.info("ingestion_write_started", row_count=len(frame))
        self._service.write_values(frame)
