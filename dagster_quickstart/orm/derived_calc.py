"""Vectorized calculators for derived (parent-based) time series."""

from typing import Callable, Dict, List

import numpy as np
import pandas as pd

from dagster_quickstart.orm.schema.constants import CALCULATION_FORMULA_TYPES

PartitionCalculator = Callable[[pd.DataFrame, List[str]], pd.Series]


def calc_spread(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[0]] - sub[cols[1]]


def calc_fly(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[0]] - 2.0 * sub[cols[1]] + sub[cols[2]]


def calc_box(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[0]] - sub[cols[1]] - sub[cols[2]] + sub[cols[3]]


def calc_ratio(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    denom = sub[cols[1]]
    out = sub[cols[0]].div(denom)
    return out.mask(denom == 0)


def calc_spread_inv(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    return sub[cols[1]] - sub[cols[0]]


def calc_ratio_inv(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    denom = sub[cols[0]]
    out = sub[cols[1]].div(denom)
    return out.mask(denom == 0)


def calc_log(sub: pd.DataFrame, cols: List[str]) -> pd.Series:
    """Log level → log diff → 21-period rolling stdev, annualized and scaled by 100."""
    x = sub[cols[0]].astype("float64")
    x = x.loc[np.isfinite(x) & (x > 0)]
    if x.empty:
        return pd.Series(dtype="float64")
    log_x = np.log(x)
    d1 = log_x.diff(1)
    out = d1.rolling(window=21, min_periods=21).std(ddof=1) * np.sqrt(252.0) * 100.0
    out = out.replace([np.inf, -np.inf], np.nan).dropna()
    return out


CALCULATORS_BY_TYPE: Dict[str, PartitionCalculator] = {
    "SPREAD": calc_spread,
    "FLY": calc_fly,
    "BOX": calc_box,
    "RATIO": calc_ratio,
    "SPREAD_INV": calc_spread_inv,
    "RATIO_INV": calc_ratio_inv,
    "LOG": calc_log,
}

if set(CALCULATORS_BY_TYPE) != set(CALCULATION_FORMULA_TYPES):
    raise RuntimeError("CALCULATORS_BY_TYPE keys must match CALCULATION_FORMULA_TYPES keys exactly")


def parse_parent_series_codes(parent_series_code_str: str) -> List[str]:
    if not parent_series_code_str or pd.isna(parent_series_code_str):
        return []
    return [code.strip() for code in str(parent_series_code_str).split("|") if code.strip()]


def compute_derived_series(
    calc_type: str,
    parent_pivot: pd.DataFrame,
    parent_series_codes: List[str],
) -> pd.Series:
    """Apply the vectorized formula for ``calc_type`` on aligned parent columns."""
    calc_type_upper = str(calc_type).strip().upper()
    calculator = CALCULATORS_BY_TYPE.get(calc_type_upper)
    if calculator is None:
        raise ValueError(f"No calculator registered for calc_type {calc_type_upper!r}")

    required_count = CALCULATION_FORMULA_TYPES.get(calc_type_upper)
    if required_count is None:
        raise ValueError(f"Unknown calc_type {calc_type_upper!r}")
    if len(parent_series_codes) != required_count:
        raise ValueError(
            f"calc_type {calc_type_upper} requires {required_count} parent series, "
            f"got {len(parent_series_codes)}"
        )

    cols = parent_series_codes
    missing = [c for c in cols if c not in parent_pivot.columns]
    if missing:
        return pd.Series(dtype="float64")

    sub = parent_pivot[cols].dropna(how="any")
    if sub.empty:
        return pd.Series(dtype="float64")

    out = calculator(sub, cols)
    return out.astype("float64")
