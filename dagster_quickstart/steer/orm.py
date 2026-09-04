"""DuckLake silver/gold schema/table names, and SteerResult persistence, for the STEER pipeline.

Silver/gold aren't a bronze/silver/gold convention DuckLake (rewrite/data_api/) has any notion
of on its own -- just metadata/values/metadata_derived tables. STEER adds silver/gold as real
DuckDB schemas inside the *same* DuckLake catalog the rest of the app already uses, via
DataAPI.read_table()/.write_table() (rewrite.data_api.api.data_api) -- see
rewrite.data_api.repositories.generic_table_repository.GenericTableRepository for how those
read/write without a second DuckLake attach and without ever rewriting an existing row.

save_result()/load_result() own the actual read/write logic for SteerResult's 2 gold tables;
SteerResult.save()/.load() (steer/analytics/results.py) are thin delegates to these, via a
function-body import -- analytics/ may not import this module at module level (this module is
above analytics/ in the import direction: constants, errors -> source/ -> analytics/ -> config,
orm, model -> run), so the delegation has to run the other way: this module imports SteerResult
at module level (to build/accept one), and SteerResult.save()/.load() import save_result()/
load_result() only at call time, once both modules have finished loading.

IMPORTANT -- universe/variant naming mismatch, deliberate: every Python-level name for the
G10/EM/CHN axis was renamed `variant` (StrategyConfig.variant, SteerResult.variant, etc.), but
the persisted column in every gold table (steer_estimates, steer_signals, steer_results,
steer_result_summary) is still literally named `universe` -- gold.steer_estimates/
gold.steer_signals already hold real rows (written by assets/steer/estimate_asset.py and
signal_asset.py, not by this module) under that column name, and DataAPI.write_table() widens
tables *by column name*: writing `variant` while a table still has `universe` would produce
both columns, each half-populated, with no error raised, and every pre-rename snapshot would
silently stop loading. save_result()/load_result() are the ONLY place that mismatch is bridged
-- they read/write the DataFrame column as "universe" and the SteerResult attribute as
`.variant`, so every other module in this package can use `variant` consistently and never has
to know the physical column disagrees with it.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from dagster_quickstart.availability.storage import latest_snapshot
from dagster_quickstart.steer.analytics.results import SteerResult

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

STEER_ESTIMATES_TABLE = "steer_estimates"
STEER_SIGNALS_TABLE = "steer_signals"
#: SteerResult's 2 tables -- see steer/analytics/results.py's module docstring.
#: steer_results is long-form (one row per series_code/as_of/date);
#: steer_result_summary is one row per series_code/as_of (z_score,
#: upper/lower, and every coefficient/standard_error/p_value, flattened).
STEER_RESULTS_TABLE = "steer_results"
STEER_RESULT_SUMMARY_TABLE = "steer_result_summary"

#: to_frame()'s fixed (non-driver) time-series columns -- everything else
#: in a loaded gold.steer_results row is a driver series (see load_result()).
#: fair_value is included here (not a SteerResult dataclass field -- it's derived from
#: fitted/is_logged via the fair_value property) purely so driver inference below
#: doesn't mistake it for a driver column.
_FIXED_TIMESERIES_COLUMNS = (
    "spot",
    "response",
    "fitted",
    "residual",
    "upper_bound",
    "lower_bound",
    "fair_value",
)


def save_result(result: SteerResult, data_api: Any) -> None:
    """Write one SteerResult via data_api.write_table() -- see module docstring.

    Writes to gold.steer_results (the time-series fields, via
    result.to_frame()) and gold.steer_result_summary (the scalar fields, via
    result.cross_section()) -- both keyed by series_code/variant/as_of.
    """
    timeseries = result.to_frame().copy()
    timeseries.index.name = "date"
    timeseries = timeseries.reset_index()
    timeseries.insert(0, "as_of", result.as_of)
    # Persisted column is "universe", not "variant" -- see module docstring.
    timeseries.insert(0, "universe", result.variant)
    timeseries.insert(0, "series_code", result.series_code)
    data_api.write_table(GOLD_SCHEMA, STEER_RESULTS_TABLE, timeseries)

    # cross_section() uses "variant" like every other in-memory SteerResult view -- renamed to
    # "universe" only here, at the point this row becomes a persisted gold.steer_result_summary
    # row (see module docstring).
    summary_row = result.cross_section().rename({"variant": "universe"})
    summary = pd.DataFrame([summary_row])
    data_api.write_table(GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, summary)


def load_result(
    data_api: Any, series_code: str, *, as_of: Optional[pd.Timestamp] = None
) -> SteerResult:
    """Load one pair's SteerResult back via data_api.read_table() -- see save_result()/module docstring.

    Both tables are append-only snapshots -- without `as_of`, the most
    recent snapshot for this series_code is returned; pass `as_of` to
    load a specific historical one. Raises LookupError if no snapshot
    matches. Driver columns are inferred as "everything in
    gold.steer_results that isn't spot/response/fitted/residual/
    upper_bound/lower_bound/date/as_of/universe/series_code" -- there's
    no fixed driver-name list to check against any more (see
    steer/analytics/results.py's module docstring). ("universe", not
    "variant" -- the persisted column name; see module docstring.)
    """
    summary = data_api.read_table(
        GOLD_SCHEMA, STEER_RESULT_SUMMARY_TABLE, series_code=series_code
    ).frame
    if summary.empty:
        raise LookupError(f"No SteerResult found in DuckLake for {series_code!r}.")
    summary["as_of"] = pd.to_datetime(summary["as_of"])
    if as_of is not None:
        summary = summary[summary["as_of"] == pd.Timestamp(as_of)]
        if summary.empty:
            raise LookupError(f"No SteerResult found for {series_code!r} as of {as_of}.")
        row = summary.iloc[0]
    else:
        # latest_snapshot() (dagster_quickstart.availability.storage) -- the same "most recent
        # as_of" logic the availability report's own append-only read uses.
        row = latest_snapshot(summary).iloc[0]
    row_as_of = row["as_of"]

    timeseries = data_api.read_table(
        GOLD_SCHEMA, STEER_RESULTS_TABLE, series_code=series_code
    ).frame
    timeseries["as_of"] = pd.to_datetime(timeseries["as_of"])
    timeseries = timeseries[timeseries["as_of"] == row_as_of].copy()
    timeseries["date"] = pd.to_datetime(timeseries["date"])
    timeseries = timeseries.set_index("date").sort_index()

    driver_names = [
        column
        for column in timeseries.columns
        if column not in _FIXED_TIMESERIES_COLUMNS
        and column not in ("as_of", "universe", "series_code")  # "universe" -- see module docstring
        # A column entirely NaN for this pair is padding from another
        # variant's wider driver set sharing this physical table (see
        # GenericTableRepository.write()'s column widening), not one of
        # this pair's own drivers.
        and not timeseries[column].isna().all()
    ]
    drivers = {name: timeseries[name] for name in driver_names}

    def _prefixed(prefix: str) -> pd.Series:
        # pd.notna(v) drops padding from another variant's wider
        # driver set sharing this physical table (see the driver_names
        # comment above) -- not a real coefficient/std-error/p-value
        # for this pair.
        return pd.Series(
            {
                str(k)[len(prefix) :]: v
                for k, v in row.items()
                if str(k).startswith(prefix) and pd.notna(v)
            }
        )

    def _optional_float(value: object) -> Optional[float]:
        return float(value) if pd.notna(value) else None  # type: ignore[call-overload,arg-type]

    dropped = row.get("dropped_variables")
    dropped_variables = tuple(dropped.split(",")) if isinstance(dropped, str) and dropped else ()

    return SteerResult(
        series_code=series_code,
        variant=row["universe"],  # persisted column is "universe" -- see module docstring
        as_of=row_as_of,
        is_logged=bool(row["is_logged"]),
        spot=timeseries["spot"],
        drivers=drivers,
        response=timeseries["response"],
        fitted=timeseries["fitted"],
        residual=timeseries["residual"],
        upper_bound=timeseries["upper_bound"],
        lower_bound=timeseries["lower_bound"],
        coefficient=_prefixed("coefficient_"),
        standard_error=_prefixed("standard_error_"),
        p_values=_prefixed("p_value_"),
        z_score=float(row["z_score"]),
        dropped_variables=dropped_variables,
        cointegration_passed=(
            bool(row["cointegration_passed"]) if pd.notna(row.get("cointegration_passed")) else None
        ),
        upper=_optional_float(row.get("upper")),
        lower=_optional_float(row.get("lower")),
        markov_state=row["markov_state"] if pd.notna(row.get("markov_state")) else None,
    )
