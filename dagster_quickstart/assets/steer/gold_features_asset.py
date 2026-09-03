"""Gold layer: build_steer_features -- this variant's driver columns, model-ready, for every pair.

Validated with steer_features_schema(strategy_config.drivers) (pandera) per
pair as an in-asset check -- non-null rate, numeric/finite drivers,
plausible bounds -- so bad data for one pair fails that pair's rows loudly
(excluded from the output, counted in the check) rather than silently
reaching the OLS; other pairs in the same variant are unaffected.
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
from dagster_quickstart.steer.analytics.results import steer_features_schema
from dagster_quickstart.steer.source.features import SERIES_CODE_COLUMN

CHECK_NAME = "validate_steer_features"


@asset(
    name="steer_features",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"steer_config"},
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="steer_features",
            description="Pandera validation of the 5 STEER driver columns, per pair (non-null, numeric, plausible bounds).",
        )
    ],
    group_name="steer",
)
def steer_features(context: AssetExecutionContext, steer_silver_prices: pd.DataFrame):
    """Build every pair's STEER feature table (5 drivers + realized_volatility + is_logged) from conformed silver prices.

    Wraps steer.features.build_steer_features with this variant's
    logged_rate_threshold/logged_rate_vol_window_days (from StrategyConfig
    -- never hardcoded here), once per pair present in steer_silver_prices.
    """
    from dagster_quickstart.steer.source.features import build_steer_features

    if steer_silver_prices.empty:
        yield AssetCheckResult(
            check_name=CHECK_NAME, passed=True, description="Nothing to validate."
        )
        yield Output(steer_silver_prices, metadata={"row_count": 0})
        return

    strategy_config = context.resources.steer_config.for_variant(context.partition_key)
    features_schema = steer_features_schema(strategy_config.drivers)

    feature_frames = []
    failing_pairs = []
    failure_details = []

    for series_code, group in steer_silver_prices.groupby(SERIES_CODE_COLUMN):
        pair_raw = group.drop(columns=[SERIES_CODE_COLUMN])
        features = build_steer_features(
            pair_raw,
            drivers=strategy_config.drivers,
            logged_rate_threshold=strategy_config.logged_rate_threshold,
            vol_window_days=strategy_config.logged_rate_vol_window_days,
        )
        try:
            features_schema.validate(features, lazy=True)
        except pa.errors.SchemaErrors as exc:
            failing_pairs.append(series_code)
            failure_details.extend(exc.failure_cases.head(5).astype(str).to_dict("records"))
            continue

        features = features.copy()
        features[SERIES_CODE_COLUMN] = series_code
        feature_frames.append(features)

    combined = pd.concat(feature_frames) if feature_frames else pd.DataFrame()

    yield AssetCheckResult(
        check_name=CHECK_NAME,
        passed=len(failing_pairs) == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"{len(failing_pairs)} pair(s) failed pandera validation: {failing_pairs}"
            if failing_pairs
            else "All pairs passed pandera validation."
        ),
        metadata={"failing_pairs": failing_pairs, "failure_details": failure_details[:10]},
    )

    yield Output(
        combined,
        metadata={
            "pair_count": combined[SERIES_CODE_COLUMN].nunique() if not combined.empty else 0,
            "row_count": len(combined),
            "logged_share": float(combined["is_logged"].mean()) if not combined.empty else 0.0,
            "preview": MetadataValue.md(combined.tail(10).to_markdown())
            if not combined.empty
            else MetadataValue.md("(no data)"),
        },
    )
