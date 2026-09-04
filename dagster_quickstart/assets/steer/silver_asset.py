"""Silver layer: materialize this variant's steer.source.features.build_silver_frame result.

One partition per variant (G10/EM/CHN, static -- see partitions.py).
Depends on fx_data_availability (same partition keys -- see
assets/availability_asset.py's FX_AVAILABILITY_PARTITIONS -- so Dagster aligns
the partition and the IO manager hands this asset the upstream frame
directly -- no manual S3/Parquet read) instead of re-discovering pairs and
re-resolving every driver role itself, so this asset's runtime never
duplicates fx_data_availability's (role, currency) resolution work --
this asset's own runtime is that one plus a single get_values() call.

All domain logic (collecting series codes, loading DriverValues, resolving
the CHN flows cutover, iterating pairs, skipping blocked/stale ones,
conforming, tagging with series_code) lives in steer.source.features.build_silver_frame
-- this asset just reads the partition key/resources, calls it, and maps
the result onto AssetCheckResult/Output.

Yields Output (not MaterializeResult) since steer_features downstream
consumes this asset's DataFrame directly as an input.
"""

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
)

from dagster_quickstart.assets.steer.freshness_check import FRESHNESS_CHECK_NAME
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS


@asset(
    name="steer_silver_prices",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"rewrite_data_api", "steer_config"},
    check_specs=[
        AssetCheckSpec(
            name=FRESHNESS_CHECK_NAME,
            asset="steer_silver_prices",
            description="Fails (WARN) if any pair in this variant isn't fresh as of the run date.",
        )
    ],
    group_name="steer",
)
def steer_silver_prices(context: AssetExecutionContext, fx_data_availability: pd.DataFrame):
    """Fetch this variant's every pair's rate + drivers from DuckLake and conform them.

    See steer.source.features.build_silver_frame's docstring for what "blocked"/
    "stale" mean and why a pair is skipped rather than passed through
    partial.
    """
    from dagster_quickstart.availability.report import pairs_from_availability_report
    from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC
    from dagster_quickstart.steer.source.features import build_silver_frame

    variant = context.partition_key
    data_api = context.resources.rewrite_data_api.api
    strategy_config = context.resources.steer_config.for_variant(variant)

    availabilities = pairs_from_availability_report(fx_data_availability, STEER_AVAILABILITY_SPEC)
    as_of = pd.Timestamp.utcnow().tz_localize(None).normalize()

    result = build_silver_frame(data_api, variant, strategy_config, availabilities, as_of=as_of)

    if result.chn_flows_cutover_error:
        context.log.warning(
            f"Could not resolve CHN flows cutover: {result.chn_flows_cutover_error}"
        )
    for series_code, reason in result.skipped_reasons.items():
        context.log.info(f"Skipping {series_code} -- {reason}")

    yield AssetCheckResult(
        check_name=FRESHNESS_CHECK_NAME,
        passed=len(result.stale_pairs) == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"{len(result.stale_pairs)} of {result.pair_count} pair(s) stale as of {as_of.date()}."
            + (f" Stale: {result.stale_pairs}" if result.stale_pairs else "")
        ),
        metadata={
            "pair_count": result.pair_count,
            "stale_pairs": result.stale_pairs,
            "blocked_pairs": result.blocked_pairs,
        },
    )

    yield Output(
        result.frame,
        metadata={
            "variant": variant,
            "pair_count": result.pair_count,
            "fetched_pair_count": result.fetched_pair_count,
            "blocked_pair_count": len(result.blocked_pairs),
            "stale_pair_count": len(result.stale_pairs),
            "row_count": len(result.frame),
            "preview": MetadataValue.md(result.frame.tail(10).to_markdown())
            if not result.frame.empty
            else MetadataValue.md("(no data available for this variant)"),
        },
    )
