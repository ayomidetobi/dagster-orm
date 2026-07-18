"""Pandera DataFrame schemas for the rewrite package."""

from __future__ import annotations

import pandas as pd
import pandera as pa
import structlog

from rewrite.data_api.columns import MetadataColumns, ValueColumns
from rewrite.data_api.errors import FrameValidationError

logger = structlog.get_logger(__name__)

METADATA_FRAME_SCHEMA = pa.DataFrameSchema(
    {
        MetadataColumns.SERIES_CODE: pa.Column(pa.String, nullable=False, required=True),
        MetadataColumns.SERIES_NAME: pa.Column(pa.String, nullable=True, required=False),
    },
    strict=False,
    coerce=True,
)

DERIVED_METADATA_FRAME_SCHEMA = pa.DataFrameSchema(
    {
        MetadataColumns.SERIES_CODE: pa.Column(pa.String, nullable=False, required=True),
        MetadataColumns.PARENT_SERIES_CODE: pa.Column(pa.String, nullable=False, required=True),
        MetadataColumns.CALC_TYPE: pa.Column(pa.String, nullable=False, required=True),
    },
    strict=False,
    coerce=True,
)

VALUE_FRAME_SCHEMA = pa.DataFrameSchema(
    {
        ValueColumns.TICKER_SOURCE: pa.Column(pa.String, nullable=True, required=False),
        ValueColumns.SERIES_CODE: pa.Column(pa.String, nullable=False, required=True),
        ValueColumns.TIMESTAMP: pa.Column(pa.DateTime, nullable=False, required=True),
        ValueColumns.VALUE: pa.Column(pa.Object, nullable=True, required=False),
    },
    strict=False,
    coerce=True,
)


def _validate(schema: pa.DataFrameSchema, frame: pd.DataFrame, *, frame_type: str) -> pd.DataFrame:
    """Validate a frame against a pandera schema, raising a domain-specific error on failure."""

    try:
        return schema.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        logger.warning("frame_validation_failed", frame_type=frame_type, errors=str(exc))
        raise FrameValidationError(f"Invalid {frame_type} frame: {exc}") from exc


def validate_metadata_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate a metadata DataFrame with Pandera."""
    return _validate(METADATA_FRAME_SCHEMA, frame, frame_type="metadata")


def validate_derived_metadata_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate a metadata_derived (series_dependencies) DataFrame with Pandera."""
    return _validate(DERIVED_METADATA_FRAME_SCHEMA, frame, frame_type="metadata_derived")


def validate_value_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate a values DataFrame with Pandera."""
    return _validate(VALUE_FRAME_SCHEMA, frame, frame_type="value")
