"""cointegration_test: Engle-Granger cointegration between each pair's rate and its OLS-fitted STEER value.

as_of defaults to "today" (the run date) -- pass a StrategyRunConfig(as_of=...)
to backfill a specific historical date without needing a date partition
axis (see assets/steer/config.py and the static universe-only partition
scheme in assets/steer/partitions.py). One universe partition covers every
pair in that universe -- this loops over each pair present in
steer_features and produces one row per pair.
"""

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from dagster_quickstart.assets.steer.config import StrategyRunConfig
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS
from dagster_quickstart.assets.steer.silver_asset import SERIES_CODE_COLUMN
from dagster_quickstart.steer.errors import InsufficientDataError


def _resolve_as_of(run_config: StrategyRunConfig, features: pd.DataFrame) -> pd.Timestamp:
    if run_config.as_of is not None:
        return pd.Timestamp(run_config.as_of)
    return pd.Timestamp(features.index.max())


@asset(
    name="steer_cointegration",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"steer_config"},
    group_name="steer",
)
def steer_cointegration(
    context: AssetExecutionContext, config: StrategyRunConfig, steer_features: pd.DataFrame
):
    """Engle-Granger cointegration test (statsmodels.tsa.stattools.coint) between each pair's rate and its fitted STEER value.

    See steer.estimation's module docstring for why this collapses the
    5-driver regression to a fitted value first rather than passing all 5
    drivers to coint() directly (coint() is bivariate). A pair with too
    little history for the trailing window gets passed=False,
    reason="insufficient_data" (not a crash) -- one pair's data gap
    doesn't affect any other pair in the same universe partition.
    """
    from dagster_quickstart.steer.estimation import cointegration_test

    if steer_features.empty:
        yield Output(pd.DataFrame(), metadata={"pair_count": 0})
        return

    strategy_config = context.resources.steer_config.for_universe(context.partition_key)
    driver_columns = [
        column
        for column in steer_features.columns
        if column not in (SERIES_CODE_COLUMN, "is_logged", "realized_volatility", "rate")
    ]

    rows = []
    for series_code, group in steer_features.groupby(SERIES_CODE_COLUMN):
        pair_features = group.drop(columns=[SERIES_CODE_COLUMN])
        as_of = _resolve_as_of(config, pair_features)
        is_logged = bool(pair_features["is_logged"].loc[:as_of].iloc[-1])

        try:
            result = cointegration_test(
                pair_features["rate"],
                pair_features[driver_columns],
                as_of=as_of,
                window_months=strategy_config.window_months,
                is_logged=is_logged,
                significance=strategy_config.cointegration_significance,
                min_observations=strategy_config.min_observations,
            )
        except InsufficientDataError as exc:
            context.log.warning(str(exc))
            rows.append(
                {
                    SERIES_CODE_COLUMN: series_code,
                    "as_of": str(as_of),
                    "passed": False,
                    "p_value": None,
                    "test_statistic": None,
                    "n_obs": None,
                    "reason": "insufficient_data",
                }
            )
            continue

        rows.append(
            {
                SERIES_CODE_COLUMN: series_code,
                "as_of": str(result.as_of),
                "passed": result.passed,
                "p_value": result.p_value,
                "test_statistic": result.test_statistic,
                "n_obs": result.n_obs,
                "reason": None,
            }
        )

    report = pd.DataFrame(rows)

    yield Output(
        report,
        metadata={
            "pair_count": len(report),
            "passed_count": int(report["passed"].sum()) if not report.empty else 0,
            "preview": MetadataValue.md(report.to_markdown(index=False))
            if not report.empty
            else MetadataValue.md("(no data)"),
        },
    )
