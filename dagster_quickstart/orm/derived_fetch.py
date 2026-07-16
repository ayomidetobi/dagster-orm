"""Out-of-cache derived series: fetch parents from vendor, then compute."""

from typing import Callable, List, Optional

import pandas as pd

from dagster_quickstart.orm.derived_calc import (
    compute_derived_series,
    parse_parent_series_codes,
)
from dagster_quickstart.orm.direct_source_fetch import (
    LoadMetadataRowsFn,
    empty_direct_value_df,
    filter_and_sort_direct_value_df,
    get_direct_source_values,
)
from dagster_quickstart.orm.exceptions import ValueQueryParameterError
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import MetadataColumns, TickerSource, ValueColumns
from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES

LoadDerivedDependencyRowsFn = Callable[[List[str]], pd.DataFrame]


def get_derived_out_of_cache_values(
    load_primary_metadata_rows: LoadMetadataRowsFn,
    load_derived_dependency_rows: LoadDerivedDependencyRowsFn,
    derived_series_codes: List[str],
    tickersource: TickerSource,
    params: Optional[ValueQueryParams],
) -> pd.DataFrame:
    """Fetch parent series from the direct vendor source and compute derived values.

    Args:
        load_primary_metadata_rows: Resolves parent ``series_code`` → vendor ticker/field
            from the primary ``metadata`` catalog.
        load_derived_dependency_rows: Loads ``metadata_derived`` rows for requested codes.
        derived_series_codes: Derived series to materialize on the fly.
        tickersource: Vendor used for parent history (Bloomberg, Hawkeye, MDS, …).
        params: Optional time window / sort / limit for parent fetch and output.

    Returns:
        Long-form DataFrame with ``series_code``, ``timestamp``, ``value`` for each derived code.
    """
    if not derived_series_codes:
        return empty_direct_value_df()

    dependencies_df = load_derived_dependency_rows(derived_series_codes)
    if dependencies_df.empty:
        return empty_direct_value_df()

    parent_codes: List[str] = []
    for _, row in dependencies_df.iterrows():
        parent_codes.extend(
            parse_parent_series_codes(row.get(MetadataColumns.PARENT_SERIES_CODE, ""))
        )
    parent_codes = list(dict.fromkeys(parent_codes))
    if not parent_codes:
        return empty_direct_value_df()

    parent_values = get_direct_source_values(
        load_primary_metadata_rows,
        parent_codes,
        tickersource,
        params,
    )
    if parent_values.empty:
        return empty_direct_value_df()

    parent_pivot = parent_values.pivot(
        index=ValueColumns.TIMESTAMP,
        columns=ValueColumns.SERIES_CODE,
        values=ValueColumns.VALUE,
    ).sort_index()

    result_frames: List[pd.DataFrame] = []
    for _, row in dependencies_df.iterrows():
        series_code = row.get(MetadataColumns.SERIES_CODE)
        if series_code is None or pd.isna(series_code):
            continue
        series_code = str(series_code).strip()
        if series_code not in derived_series_codes:
            continue

        calc_type = row.get(MetadataColumns.CALC_TYPE, "")
        parent_series_codes = parse_parent_series_codes(
            row.get(MetadataColumns.PARENT_SERIES_CODE, "")
        )
        if not calc_type or not parent_series_codes:
            continue

        calc_type_upper = str(calc_type).strip().upper()
        if calc_type_upper not in CALCULATION_FORMULA_TYPES:
            raise ValueQueryParameterError(
                f"Unknown calc_type '{calc_type}' for derived series '{series_code}'"
            )

        try:
            derived_series = compute_derived_series(
                calc_type_upper, parent_pivot, parent_series_codes
            )
        except ValueError as exc:
            raise ValueQueryParameterError(str(exc)) from exc

        if derived_series.empty:
            continue

        derived_series.name = series_code
        long = derived_series.reset_index()
        long.columns = [ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        long[ValueColumns.SERIES_CODE] = series_code
        result_frames.append(
            long[[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]]
        )

    if not result_frames:
        return empty_direct_value_df()

    out = pd.concat(result_frames, ignore_index=True)
    return filter_and_sort_direct_value_df(out, params)
