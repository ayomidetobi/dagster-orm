"""Pandera-based value quality validation for ingest_bloomberg_values.

Runs as an in-asset check (see asset.py's check_specs), validating directly
against the wide-form values_df data_api.get_values() already returned -- no
second DuckLake query needed. Columns are dynamic series_codes (not fixed
names like the metadata schema), so checks are matched by regex across every
column rather than declared one by one.
"""

import numpy as np
import pandas as pd
import pandera as pa
from dagster import AssetCheckResult, AssetCheckSeverity

CHECK_NAME = "validate_bloomberg_values_quality"
MAX_FAILURE_DETAILS = 10


def _not_all_null(series: pd.Series) -> bool:
    """A series with zero real data means the vendor fetch effectively failed for it."""

    return not series.isna().all()


def _all_finite(series: pd.Series) -> bool:
    """A calculation gone wrong can produce inf/-inf; a real market value never is."""

    return bool(np.isfinite(series.dropna()).all())


#: Add another Check(...) here for any additional value-quality rule --
#: matched against every series_code column since column names are dynamic.
VALUES_QUALITY_SCHEMA = pa.DataFrameSchema(
    columns={
        ".*": pa.Column(
            float,
            nullable=True,
            regex=True,
            checks=[
                pa.Check(_not_all_null, error="series has no data at all"),
                pa.Check(_all_finite, error="series contains a non-finite (inf/-inf) value"),
            ],
        )
    },
    index=pa.Index(
        pa.DateTime,
        unique=True,
        checks=pa.Check(
            lambda index: index <= pd.Timestamp.now(),
            error="timestamp is in the future",
        ),
    ),
    coerce=True,
    strict=False,
)


def build_values_quality_check_result(frame: pd.DataFrame | None, *, log) -> AssetCheckResult:
    """Validate `frame` (the wide-form values_df) with pandera and build the check result.

    frame=None or empty means nothing was fetched -- reported as a trivial
    pass (nothing to validate), not a failure. log is anything with an
    .error(str) method -- AssetExecutionContext.log qualifies.
    """
    if frame is None or frame.empty:
        return AssetCheckResult(
            check_name=CHECK_NAME,
            passed=True,
            description="No values fetched -- nothing to validate",
            metadata={"series_count": 0, "timestamp_count": 0, "failure_count": 0},
        )

    series_count = len(frame.columns)
    timestamp_count = len(frame)

    try:
        VALUES_QUALITY_SCHEMA.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        # An index-level check has no "column" -- dedupe on the combination
        # that's actually meaningful for each context.
        failures = exc.failure_cases.drop_duplicates(subset=["check", "column", "index"])
        failure_count = len(failures)

        for _, row in failures.head(MAX_FAILURE_DETAILS).iterrows():
            label = row["column"] if row["schema_context"] == "Column" else "<timestamp index>"
            log.error(
                f"{label} @ {row['index']}: failed check '{row['check']}': "
                f"{row['failure_case']!r}"
            )

        description = (
            f"Found {failure_count} pandera validation failure(s) across "
            f"{series_count} series / {timestamp_count} timestamp(s)"
        )
        if failure_count > MAX_FAILURE_DETAILS:
            description += f" (showing first {MAX_FAILURE_DETAILS})"

        return AssetCheckResult(
            check_name=CHECK_NAME,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=description,
            metadata={
                "series_count": series_count,
                "timestamp_count": timestamp_count,
                "failure_count": failure_count,
                "failure_details": failures.head(MAX_FAILURE_DETAILS)
                .astype(str)
                .to_dict("records"),
            },
        )

    return AssetCheckResult(
        check_name=CHECK_NAME,
        passed=True,
        description=(
            f"All {series_count} series / {timestamp_count} timestamp(s) passed "
            "pandera validation"
        ),
        metadata={
            "series_count": series_count,
            "timestamp_count": timestamp_count,
            "failure_count": 0,
        },
    )
