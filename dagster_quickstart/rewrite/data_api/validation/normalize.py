"""Frame normalization applied on import, ahead of schema validation."""

from __future__ import annotations

import numpy as np
import pandas as pd


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
