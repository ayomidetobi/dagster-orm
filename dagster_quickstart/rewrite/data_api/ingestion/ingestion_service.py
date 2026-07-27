"""Ingestion orchestration."""

from __future__ import annotations

import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.services.vendor_service import VendorService
from dagster_quickstart.rewrite.data_api.ingestion.writer import IngestionWriter

logger = structlog.get_logger(__name__)


class IngestionService:
    """Orchestrate vendor fetch, normalization, and persistence."""

    def __init__(self, vendor_service: VendorService, writer: IngestionWriter):
        """Initialize the ingestion service."""
        self._vendor_service = vendor_service
        self._writer = writer

    def ingest(self, vendor: str, **kwargs: object) -> pd.DataFrame:
        """Fetch a normalized frame from a vendor and persist it."""
        logger.info("ingestion_started", vendor=vendor)
        frame = self._vendor_service.fetch(vendor, **kwargs)
        if not frame.empty:
            self._writer.write(frame)
        logger.info("ingestion_completed", vendor=vendor, row_count=len(frame))
        return frame
