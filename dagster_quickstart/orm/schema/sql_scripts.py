"""SQL query templates for validation and data operations.

This module contains reusable SQL query templates that can be used across
the codebase. Queries use placeholders that should be replaced with actual
values before execution.
"""

VALIDATE_PARENT_SERIES_COUNT_QUERY = """
    WITH validated_data AS (
        SELECT
            COALESCE(parent_series_code, '') AS parent_series_code,
            COALESCE(child_series_code, '') AS child_series_code,
            UPPER(COALESCE(calc_type, '')) AS calc_type,
            len(
                list_filter(
                    string_split(COALESCE(parent_series_code, ''), '|'),
                    x -> length(trim(x)) > 0
                )
            ) AS parent_count,
            CASE
                {case_statements}
                ELSE NULL
            END AS required_count
        FROM {parquet_source}
    )
    SELECT
        child_series_code,
        calc_type,
        parent_count,
        required_count,
        CASE
            WHEN required_count IS NULL THEN 'Unknown calc_type: ' || calc_type
            ELSE 'Expected ' || CAST(required_count AS VARCHAR) || ' parent series, found ' || CAST(parent_count AS VARCHAR)
        END AS error
    FROM validated_data
    WHERE required_count IS NULL OR parent_count != required_count
"""
