"""generate_signal: BUY/SELL/NONE -> one gold.steer_signals row per pair.

Kept as its own table (not folded into gold.steer_estimates) so trading-rule
parameters (z_threshold, stop_reward_ratio) can be iterated on in a
backtest without re-running the model layer -- per the output-tables spec.
One universe partition covers every pair in that universe -- this loops
over each pair present in steer_estimate and writes every pair's row to
gold.steer_signals in one call.
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

from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS
from dagster_quickstart.assets.steer.silver_asset import SERIES_CODE_COLUMN
from dagster_quickstart.steer.estimation import CointegrationResult, SteerEstimate
from dagster_quickstart.steer.schemas import STEER_SIGNALS_SCHEMA
from dagster_quickstart.steer.storage import GOLD_SCHEMA, STEER_SIGNALS_TABLE

CHECK_NAME = "validate_steer_signals"


@asset(
    name="steer_signal",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"steer_config", "steer_catalog"},
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="steer_signal",
            description="Pandera validation of the gold.steer_signals rows about to be written.",
        )
    ],
    group_name="steer",
)
def steer_signal(
    context: AssetExecutionContext,
    steer_features: pd.DataFrame,
    steer_estimate: pd.DataFrame,
    steer_cointegration: pd.DataFrame,
):
    """BUY/SELL/NONE for every pair, from steer_estimate + steer_cointegration -- see steer.signals.generate_signal."""
    from dagster_quickstart.steer.signals import generate_signal

    universe = context.partition_key

    if steer_estimate.empty:
        yield AssetCheckResult(
            check_name=CHECK_NAME, passed=True, description="Nothing to validate."
        )
        yield Output(pd.DataFrame(), metadata={"pair_count": 0})
        return

    strategy_config = context.resources.steer_config.for_universe(universe)
    cointegration_by_pair = steer_cointegration.set_index(SERIES_CODE_COLUMN)

    rows = []
    for _, estimate_row in steer_estimate.iterrows():
        series_code = estimate_row[SERIES_CODE_COLUMN]
        if series_code not in cointegration_by_pair.index:
            context.log.warning(f"No cointegration result for {series_code} -- skipping signal.")
            continue

        estimate = SteerEstimate(
            as_of=pd.Timestamp(estimate_row["date"]),
            is_logged=bool(estimate_row["is_logged"]),
            coefficients={},
            fitted_value=float(estimate_row["fitted_value"]),
            actual_value=float(estimate_row["actual_value"]),
            residual_std=0.0,
            z_score=float(estimate_row["z_score"]),
            r_squared=float(estimate_row["r_squared"]),
            n_obs=int(estimate_row["n_obs"]),
        )
        cointegration_row = cointegration_by_pair.loc[series_code]
        cointegration = CointegrationResult(
            as_of=estimate.as_of,
            passed=bool(cointegration_row["passed"]),
            p_value=float(cointegration_row.get("p_value") or 1.0),
            test_statistic=float(cointegration_row.get("test_statistic") or 0.0),
            critical_values=(0.0, 0.0, 0.0),
            n_obs=int(cointegration_row.get("n_obs") or 0),
        )
        current_rate = float(
            steer_features.loc[steer_features[SERIES_CODE_COLUMN] == series_code, "rate"]
            .loc[: estimate.as_of]
            .iloc[-1]
        )

        signal = generate_signal(
            estimate,
            cointegration,
            current_rate=current_rate,
            z_threshold=strategy_config.z_threshold,
            stop_reward_ratio=strategy_config.stop_reward_ratio,
        )

        rows.append(
            {
                "date": signal.as_of,
                "universe": universe,
                SERIES_CODE_COLUMN: series_code,
                "signal": signal.signal,
                "entry_z_score": signal.entry_z_score,
                "target": signal.target,
                "stop_loss": signal.stop_loss,
                "reason": signal.reason,
            }
        )

    row_df = pd.DataFrame(rows)

    if row_df.empty:
        yield AssetCheckResult(check_name=CHECK_NAME, passed=True, description="Nothing to write.")
        yield Output(row_df, metadata={"pair_count": 0})
        return

    try:
        STEER_SIGNALS_SCHEMA.validate(row_df, lazy=True)
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

    context.resources.steer_catalog.catalog.write(GOLD_SCHEMA, STEER_SIGNALS_TABLE, row_df)

    yield Output(
        row_df,
        metadata={
            "pair_count": len(row_df),
            "signal_counts": row_df["signal"].value_counts().to_dict(),
            "preview": MetadataValue.md(row_df.to_markdown(index=False)),
        },
    )
