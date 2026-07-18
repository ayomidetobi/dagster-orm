"""Helpers for building filter option payloads from DataFrames."""

from typing import Dict, List, Optional, Union

import pandas as pd

from dagster_quickstart.orm.exceptions import InvalidFilterFieldError


def dataframe_filter_options(
    dataframe: pd.DataFrame,
    fields: Optional[Union[str, List[str]]] = None,
    *,
    as_dataframe: bool = False,
) -> Union[List[str], Dict[str, List[str]], pd.DataFrame]:
    """Build unique option values from a DataFrame.

    Args:
        dataframe: Source DataFrame whose columns provide filter values.
        fields: Target field name, list of field names, or ``None`` for all columns.
        as_dataframe: When ``True``, return a normalized two-column DataFrame with
            ``field`` and ``value`` columns.

    Returns:
        Distinct values for a single field, a mapping for multiple fields, or a
        normalized DataFrame when ``as_dataframe=True``.

    Raises:
        ValueError: If ``fields`` is an empty list.
        InvalidFilterFieldError: If any requested fields are missing from ``dataframe``.
    """
    if fields is None:
        requested_fields = list(dataframe.columns)
    else:
        requested_fields = [fields] if isinstance(fields, str) else list(fields)

    if not requested_fields:
        raise ValueError("filter_options() requires at least one field")

    available_fields = set(dataframe.columns)
    invalid_fields = [field for field in requested_fields if field not in available_fields]
    if invalid_fields:
        raise InvalidFilterFieldError(
            f"Invalid field(s): {invalid_fields}. " f"Available fields: {sorted(available_fields)}"
        )

    options_by_field: Dict[str, List[str]] = {}
    for field in requested_fields:
        values = dataframe[field].dropna().astype(str).map(str.strip)
        options_by_field[field] = [value for value in pd.unique(values) if value]

    if as_dataframe:
        rows = [
            {"field": field, "value": value}
            for field, values in options_by_field.items()
            for value in values
        ]
        return pd.DataFrame(rows, columns=["field", "value"])

    if fields is not None and len(requested_fields) == 1:
        return options_by_field[requested_fields[0]]

    return options_by_field
