"""Frame normalization applied on import, ahead of schema validation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import structlog

from dagster_quickstart.rewrite.data_api.columns import ValueColumns

logger = structlog.get_logger(__name__)


def strip_whitespace(frame: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from every string column.

    A whitespace-only value (``""``, ``"   "``) collapses to NaN afterward,
    so a nullable=False column (e.g. series_code) correctly rejects a blank
    code instead of silently accepting it.
    """
    frame = frame.copy()
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = frame[column].str.strip().replace("", np.nan)
    return frame


def coerce_numeric_value(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce the value column to numeric, turning non-numeric strings into NaN.

    A real vendor API returns sentinel strings ("NOT FOUND", "N/A", "#N/A", ...)
    for missing/invalid data points instead of a number -- the demo vendor
    clients in this codebase don't, but production ones do. DuckDB's
    ``value DOUBLE`` column can't hold those directly (ConversionException on
    insert), so they're normalized to NaN (a NULL double) here instead of
    failing the whole batch write over one bad point.
    """
    if ValueColumns.VALUE not in frame.columns:
        return frame

    frame = frame.copy()
    original = frame[ValueColumns.VALUE]
    coerced = pd.to_numeric(original, errors="coerce")

    newly_null = coerced.isna() & original.notna()
    if newly_null.any():
        examples = original[newly_null].astype(str).unique().tolist()[:5]
        logger.warning(
            "non_numeric_value_coerced_to_null",
            count=int(newly_null.sum()),
            examples=examples,
        )

    frame[ValueColumns.VALUE] = coerced
    return frame
