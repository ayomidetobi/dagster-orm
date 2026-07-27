"""Metadata quality reporting, sourced entirely from the DuckLake catalog itself.

Rather than validating against a separate, static reference file, this diffs
the metadata table's current state against its own immediately-preceding
DuckLake snapshot -- so "is this column/value new" is answered from the
catalog's real import history, which grows automatically with every import.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from dagster_quickstart.rewrite.data_api.columns import MetadataColumns

#: Columns checked for null values by default -- add more here as needed
#: (e.g. a required categorical field). Not every column: an optional
#: attribute like `currency` being null for some series isn't a defect.
DEFAULT_NULL_CHECK_COLUMNS: tuple[str, ...] = (
    MetadataColumns.SERIES_CODE,
    MetadataColumns.SERIES_NAME,
)


@dataclass(frozen=True, slots=True)
class MetadataQualityReport:
    """Data-quality snapshot for the metadata catalog.

    new_columns/new_values are informational (metadata legitimately grows
    over time) -- duplicate_series_codes/null_counts are the actual defects.
    """

    total_count: int
    current_version: int | None
    baseline_version: int | None
    null_counts: dict[str, int]
    duplicate_series_codes: pd.DataFrame
    new_columns: list[str]
    new_values: dict[str, list[str]]

    @property
    def has_duplicates(self) -> bool:
        return not self.duplicate_series_codes.empty

    @property
    def has_nulls(self) -> bool:
        return any(count > 0 for count in self.null_counts.values())

    @property
    def has_new_columns(self) -> bool:
        return bool(self.new_columns)

    @property
    def has_new_values(self) -> bool:
        return bool(self.new_values)

    @property
    def is_clean(self) -> bool:
        """True when there are no duplicates and no nulls.

        New columns/values don't affect this -- they're expected as metadata
        grows, not a defect.
        """
        return not self.has_duplicates and not self.has_nulls

    def summary(self) -> str:
        """One-line, human-readable summary for logs/descriptions."""

        parts = [f"{self.total_count} row(s)"]
        parts.append(
            f"baseline=v{self.baseline_version} -> current=v{self.current_version}"
            if self.baseline_version is not None
            else "no prior snapshot to compare against (first import)"
        )
        if self.has_duplicates:
            parts.append(f"{len(self.duplicate_series_codes)} duplicate series_code value(s)")
        if self.has_nulls:
            null_summary = ", ".join(f"{column}={count}" for column, count in self.null_counts.items())
            parts.append(f"nulls: {null_summary}")
        if self.has_new_columns:
            parts.append(f"new column(s): {', '.join(self.new_columns)}")
        if self.has_new_values:
            value_summary = "; ".join(f"{column}={values}" for column, values in self.new_values.items())
            parts.append(f"new value(s): {value_summary}")
        return "; ".join(parts)


def find_duplicate_series_codes(frame: pd.DataFrame) -> pd.DataFrame:
    """Return (series_code, duplicate_count) for every series_code appearing more than once."""

    if frame.empty or MetadataColumns.SERIES_CODE not in frame.columns:
        return pd.DataFrame(columns=[MetadataColumns.SERIES_CODE, "duplicate_count"])

    counts = frame[MetadataColumns.SERIES_CODE].value_counts()
    duplicates = counts[counts > 1]
    return pd.DataFrame(
        {MetadataColumns.SERIES_CODE: duplicates.index, "duplicate_count": duplicates.to_numpy()}
    )


def frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    """Content-equality check that's independent of row order.

    ducklake_snapshots() is catalog-wide -- it includes every table's writes,
    not just this one -- so consecutive snapshot_ids frequently leave this
    table's own content unchanged (e.g. a write to a different table). A
    plain positional DataFrame.equals() would treat any such row-order
    wobble as "different"; sorting first makes the comparison about actual
    content.
    """

    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    if left.empty:
        return True

    columns = list(left.columns)
    left_sorted = left.sort_values(columns).reset_index(drop=True)
    right_sorted = right.sort_values(columns).reset_index(drop=True)
    return left_sorted.equals(right_sorted)


#: Identifier columns are excluded from new-value reporting -- every
#: legitimate new row has a never-before-seen series_code/series_name, so
#: flagging that as "new" is just noise. A new *categorical* value (e.g. an
#: asset_class no series has ever used before) is the meaningful signal.
IDENTIFIER_COLUMNS = frozenset({MetadataColumns.SERIES_CODE, MetadataColumns.SERIES_NAME})


def build_quality_report(
    *,
    current: pd.DataFrame,
    baseline: pd.DataFrame | None,
    current_version: int | None,
    baseline_version: int | None,
    null_check_columns: Sequence[str] = DEFAULT_NULL_CHECK_COLUMNS,
) -> MetadataQualityReport:
    """Diff `current` against `baseline` (the catalog's own prior snapshot).

    `baseline=None` means there's no prior snapshot to compare against (the
    very first import) -- new_columns/new_values are reported empty rather
    than flagging every column/value as "new" on that first run.

    null_check_columns controls which columns get flagged for null values --
    defaults to DEFAULT_NULL_CHECK_COLUMNS (series_code/series_name). Not
    every column is checked: an optional attribute being null for some
    series isn't a defect.
    """

    baseline_columns = list(baseline.columns) if baseline is not None else []
    new_columns = (
        [column for column in current.columns if column not in baseline_columns]
        if baseline is not None
        else []
    )

    new_values: dict[str, list[str]] = {}
    if baseline is not None:
        shared_columns = [
            column
            for column in current.columns
            if column in baseline_columns
            and column not in new_columns
            and column not in IDENTIFIER_COLUMNS
        ]
        for column in shared_columns:
            current_values = set(current[column].dropna().astype(str).str.strip())
            baseline_values = set(baseline[column].dropna().astype(str).str.strip())
            fresh_values = sorted(current_values - baseline_values)
            if fresh_values:
                new_values[column] = fresh_values

    null_counts = {
        column: int(current[column].isna().sum())
        for column in null_check_columns
        if column in current.columns and current[column].isna().any()
    }

    return MetadataQualityReport(
        total_count=len(current),
        current_version=current_version,
        baseline_version=baseline_version,
        null_counts=null_counts,
        duplicate_series_codes=find_duplicate_series_codes(current),
        new_columns=new_columns,
        new_values=new_values,
    )
