"""Pandera-based metadata quality validation for load_meta_series_to_s3.

Runs as an in-asset check (see asset.py's check_specs), validating directly
against the validated_df data_api.import_metadata() already returned -- no
second CSV read or DuckLake query needed, since validated_df already is the
exact set of rows the current import just wrote.
"""

import pandas as pd
import pandera as pa
from dagster import AssetCheckResult, AssetCheckSeverity

from dagster_quickstart.rewrite.data_api.columns import MetadataColumns

CHECK_NAME = "validate_metadata_quality"
MAX_FAILURE_DETAILS_PER_CATEGORY = 10


def _not_blank(series: pd.Series) -> pd.Series:
    """A value that's only whitespace is effectively blank, not a real value."""

    return series.str.strip().str.len() > 0


def _bbg_ticker_field_paired(frame: pd.DataFrame) -> pd.Series:
    """bbg_ticker/bbg_field describe one Bloomberg reference -- neither is useful alone."""

    if "bbg_ticker" not in frame.columns or "bbg_field" not in frame.columns:
        return pd.Series(True, index=frame.index)
    return frame["bbg_ticker"].notna() == frame["bbg_field"].notna()


def _valid_from_before_valid_to(frame: pd.DataFrame) -> pd.Series:
    """A series' validity window must not end before it starts."""

    if "valid_from" not in frame.columns or "valid_to" not in frame.columns:
        return pd.Series(True, index=frame.index)
    valid_from = pd.to_datetime(frame["valid_from"], errors="coerce")
    valid_to = pd.to_datetime(frame["valid_to"], errors="coerce")
    both_present = valid_from.notna() & valid_to.notna()
    return ~both_present | (valid_from <= valid_to)


def _calc_type_parent_series_paired(frame: pd.DataFrame) -> pd.Series:
    """calc_type/parent_series_code describe one derived-series formula -- neither is useful alone."""

    if (
        MetadataColumns.CALC_TYPE not in frame.columns
        or MetadataColumns.PARENT_SERIES_CODE not in frame.columns
    ):
        return pd.Series(True, index=frame.index)
    return frame[MetadataColumns.CALC_TYPE].notna() == frame[MetadataColumns.PARENT_SERIES_CODE].notna()


#: Add a Column(...) entry here for any additional field that should be
#: validated -- e.g. an asset_class categorical check via Check.isin([...]).
METADATA_QUALITY_SCHEMA = pa.DataFrameSchema(
    {
        MetadataColumns.SERIES_CODE: pa.Column(
            str,
            nullable=False,
            unique=True,
            checks=pa.Check(_not_blank, error="series_code must not be blank"),
        ),
        MetadataColumns.SERIES_NAME: pa.Column(
            str,
            nullable=False,
            checks=pa.Check(_not_blank, error="series_name must not be blank"),
        ),
    },
    checks=[
        pa.Check(
            _bbg_ticker_field_paired,
            error="bbg_ticker/bbg_field must both be set or both be blank",
        ),
        pa.Check(
            _calc_type_parent_series_paired,
            error="calc_type/parent_series_code must both be set or both be blank",
        ),
        pa.Check(_valid_from_before_valid_to, error="valid_from must be <= valid_to"),
    ],
    strict=False,
    coerce=True,
)


def _series_code_for_index(frame: pd.DataFrame, index: object) -> str:
    """Best-effort series_code label for a failing row, for readable logs.

    Falls back to the row number when series_code itself is null/blank --
    exactly the row a "series_code must not be blank" failure points at --
    since printing "None"/"nan" as the label isn't useful.
    """

    try:
        value = frame.loc[index, MetadataColumns.SERIES_CODE]
        if isinstance(value, pd.Series):  # duplicate index labels -- take the first
            value = value.iloc[0]
        if pd.isna(value) or not str(value).strip():
            return f"row {index}"
        return str(value)
    except Exception:
        return f"row {index}"


#: Which failure category each pandera check belongs to, for grouped
#: reporting -- so e.g. 151 pairing failures can't crowd 1 null failure and
#: 2 duplicate failures out of the log. Order here is also the order
#: categories are logged/reported in.
_CATEGORY_LABELS: dict[str, str] = {
    "duplicate": "Duplicate series_code",
    "null": "Null/blank value",
    "pairing": "Field pairing",
    "other": "Other",
}


def _categorize_check(check_name: str) -> str:
    """Map a pandera check's name to a reporting category (see _CATEGORY_LABELS)."""

    if check_name == "field_uniqueness":
        return "duplicate"
    if check_name == "not_nullable" or check_name.endswith("must not be blank"):
        return "null"
    if "must both be set or both be blank" in check_name:
        return "pairing"
    return "other"


def build_metadata_quality_check_result(frame: pd.DataFrame, *, log) -> AssetCheckResult:
    """Validate `frame` with pandera and build the check's AssetCheckResult.

    log is anything with an .error(str) method -- both AssetExecutionContext
    and AssetCheckExecutionContext's `.log` qualify.
    """
    total_count = len(frame)

    try:
        METADATA_QUALITY_SCHEMA.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        # A dataframe-wide check (e.g. the bbg_ticker/bbg_field pairing) reports
        # one row per *column* for the same failing index -- dedupe down to one
        # entry per distinct (check, row) failure before reporting.
        failures = exc.failure_cases.drop_duplicates(subset=["check", "index"]).copy()
        failures["category"] = failures["check"].map(_categorize_check)
        failure_count = len(failures)

        counts_by_category: dict[str, int] = {}
        failure_details: list[dict[str, object]] = []
        summary_parts: list[str] = []

        for category, label in _CATEGORY_LABELS.items():
            category_failures = failures[failures["category"] == category]
            if category_failures.empty:
                continue

            counts_by_category[category] = len(category_failures)
            summary_parts.append(f"{len(category_failures)} {label.lower()}")

            log.error(f"--- {label} failures ({len(category_failures)}) ---")
            shown = category_failures.head(MAX_FAILURE_DETAILS_PER_CATEGORY)
            for _, row in shown.iterrows():
                series_code = _series_code_for_index(frame, row["index"])
                if row["schema_context"] == "Column":
                    log.error(
                        f"{series_code}: column '{row['column']}' failed check "
                        f"'{row['check']}': {row['failure_case']!r}"
                    )
                else:
                    # A dataframe-wide check isn't about one specific column, so
                    # there's no single value to show.
                    log.error(f"{series_code}: failed check '{row['check']}'")

            remaining = len(category_failures) - len(shown)
            if remaining > 0:
                log.error(f"... and {remaining} more {label.lower()} failure(s)")

            failure_details.extend(shown.drop(columns=["category"]).astype(str).to_dict("records"))

        description = (
            f"Found {failure_count} pandera validation failure(s) across {total_count} row(s): "
            + ", ".join(summary_parts)
        )

        return AssetCheckResult(
            check_name=CHECK_NAME,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=description,
            metadata={
                "total_count": total_count,
                "failure_count": failure_count,
                "failure_counts_by_category": counts_by_category,
                "failure_details": failure_details,
            },
        )

    return AssetCheckResult(
        check_name=CHECK_NAME,
        passed=True,
        description=f"All {total_count} metadata row(s) passed pandera validation",
        metadata={"total_count": total_count, "failure_count": 0},
    )
