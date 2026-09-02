"""estimate_steer: rolling OLS fit (+ sign_check_and_reestimate) -> one gold.steer_estimates row per pair.

sign_check_and_reestimate is applied *inside* this asset rather than as a
separate downstream asset -- both the raw OLS fit and the sign-check
outcome land in the same steer_estimates row (fitted STEER, coefficients,
z-score, cointegration flag, sign-drop flag all together, per the output
table spec), and the sign-check's outcome is logged as materialization
metadata (dropped_variables, sign_dropped) rather than needing its own
asset node. One universe partition covers every pair in that universe --
this loops over each pair present in steer_features and writes every
pair's row to gold.steer_estimates in one call.
"""

import pandas as pd
import pandera as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
)

from dagster_quickstart.assets.steer.config import StrategyRunConfig
from dagster_quickstart.assets.steer.cointegration_asset import _resolve_as_of
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS
from dagster_quickstart.steer.pipeline import SERIES_CODE_COLUMN
from dagster_quickstart.steer.errors import InsufficientDataError
from dagster_quickstart.steer.schemas import steer_estimates_schema
from dagster_quickstart.steer.storage import GOLD_SCHEMA, STEER_ESTIMATES_TABLE

CHECK_NAME = "validate_steer_estimates"


@asset(
    name="steer_estimate",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"steer_config", "rewrite_data_api"},
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="steer_estimate",
            description="Pandera validation of the gold.steer_estimates rows about to be written.",
        )
    ],
    group_name="steer",
)
def steer_estimate(
    context: AssetExecutionContext,
    config: StrategyRunConfig,
    steer_features: pd.DataFrame,
    steer_cointegration: pd.DataFrame,
):
    """Fit every pair's STEER value (rolling OLS + sign-check/re-estimate) and write one row per pair to gold.steer_estimates.

    See the module docstring for why sign_check_and_reestimate's outcome
    is folded into this same row/asset rather than a separate one.
    """
    from dagster_quickstart.steer.estimation import sign_check_and_reestimate

    universe = context.partition_key

    if steer_features.empty:
        yield AssetCheckResult(
            check_name=CHECK_NAME, passed=True, description="Nothing to validate."
        )
        yield Output(pd.DataFrame(), metadata={"pair_count": 0})
        return

    strategy_config = context.resources.steer_config.for_universe(universe)
    cointegration_by_pair = (
        steer_cointegration.set_index(SERIES_CODE_COLUMN)["passed"]
        if not steer_cointegration.empty
        else pd.Series(dtype=bool)
    )
    driver_columns = [
        column
        for column in steer_features.columns
        if column not in (SERIES_CODE_COLUMN, "is_logged", "realized_volatility", "rate")
    ]

    rows = []
    insufficient_data_pairs = []

    for series_code, group in steer_features.groupby(SERIES_CODE_COLUMN):
        pair_features = group.drop(columns=[SERIES_CODE_COLUMN])
        as_of = _resolve_as_of(config, pair_features)
        is_logged = bool(pair_features["is_logged"].loc[:as_of].iloc[-1])

        try:
            estimate = sign_check_and_reestimate(
                pair_features["rate"],
                pair_features[driver_columns],
                as_of=as_of,
                window_months=strategy_config.window_months,
                is_logged=is_logged,
                expected_signs=strategy_config.expected_signs,
                min_observations=strategy_config.min_observations,
            )
        except InsufficientDataError as exc:
            context.log.warning(str(exc))
            insufficient_data_pairs.append(series_code)
            continue

        if estimate.dropped_variables:
            context.log.warning(
                f"sign_check_and_reestimate dropped {estimate.dropped_variables} for "
                f"{series_code} as of {as_of} (sign contradicted expected_signs)"
            )

        rows.append(
            {
                "date": pd.Timestamp(as_of),
                "universe": universe,
                SERIES_CODE_COLUMN: series_code,
                "is_logged": estimate.is_logged,
                "const_coef": estimate.coefficients.get("const"),
                **{
                    f"{driver}_coef": estimate.coefficients.get(driver)
                    for driver in strategy_config.drivers
                },
                "fitted_value": estimate.fitted_value,
                "actual_value": estimate.actual_value,
                "z_score": estimate.z_score,
                "r_squared": estimate.r_squared,
                "n_obs": estimate.n_obs,
                "cointegration_passed": bool(cointegration_by_pair.get(series_code, False)),
                "sign_dropped": bool(estimate.dropped_variables),
                "dropped_variables": ",".join(estimate.dropped_variables) or None,
            }
        )

    row_df = pd.DataFrame(rows)

    if row_df.empty:
        yield AssetCheckResult(
            check_name=CHECK_NAME,
            passed=True,
            description=f"Nothing to write ({len(insufficient_data_pairs)} pair(s) had insufficient data).",
        )
        yield Output(row_df, metadata={"pair_count": 0})
        return

    try:
        steer_estimates_schema(strategy_config.drivers).validate(row_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases.astype(str).to_dict("records")
        yield AssetCheckResult(
            check_name=CHECK_NAME,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"{len(failures)} pandera validation failure(s)",
            metadata={"failure_details": failures},
        )
        yield Output(pd.DataFrame(), metadata={"error": "validation_failed"})
        return

    yield AssetCheckResult(
        check_name=CHECK_NAME,
        passed=True,
        description=f"{len(row_df)} row(s) passed pandera validation.",
    )

    context.resources.rewrite_data_api.api.write_table(GOLD_SCHEMA, STEER_ESTIMATES_TABLE, row_df)

    yield Output(
        row_df,
        metadata={
            "pair_count": len(row_df),
            "insufficient_data_pairs": insufficient_data_pairs,
            "sign_dropped_count": int(row_df["sign_dropped"].sum()),
            "preview": MetadataValue.md(row_df.to_markdown(index=False)),
        },
    )
