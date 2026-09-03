"""steer_data_availability: report of which pairs in this variant are blocked, and why.

One variant partition (G10/EM/CHN, same static scheme as the rest of the
graph -- see partitions.py) covers every pair in that variant in one
materialization. The check FAILS (WARN, not ERROR -- a data completeness
gap, not a broken pipeline) whenever any pair is blocked, so the gap is
visible in the Dagster UI rather than only in this asset's own metadata
table.

The report includes a flat `{leg}_{role}` column per resolved driver role
(e.g. base_swap_2y, quote_local_equity -- see
steer.source.discovery.build_availability_report), not just the blocked/reason
summary -- steer_silver_prices (which depends on this asset's output)
reconstructs each pair's PairAvailability straight from these columns via
PairAvailability.from_report_row, instead of re-resolving every role from
a fresh metadata query -- so the two assets never redo the same
resolution work for the same partition.
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

from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS

CHECK_NAME = "no_blocked_pairs"


@asset(
    name="steer_data_availability",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"rewrite_data_api"},
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="steer_data_availability",
            description="Fails (WARN) if any pair in this variant is missing genuine per-country driver data.",
        )
    ],
    group_name="steer",
)
def steer_data_availability(context: AssetExecutionContext):
    """Build the data_availability report for every pair in this variant.

    See steer/source/discovery.py's module docstring for exactly what "blocked"
    means and why local_equity/rate-differential aren't substituted with a
    global proxy when missing.
    """
    from dagster_quickstart.steer.source.discovery import build_availability_report, discover_pairs

    variant = context.partition_key
    data_api = context.resources.rewrite_data_api.api

    pairs = discover_pairs(variant, data_api)
    if pairs.empty:
        yield AssetCheckResult(
            check_name=CHECK_NAME,
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"No FX pairs discovered in the datalake for {variant}.",
        )
        yield Output(pd.DataFrame(), metadata={"pair_count": 0})
        return

    report = build_availability_report({variant: pairs}, data_api)

    blocked_count = int(report["blocked"].sum())
    total_count = len(report)

    yield AssetCheckResult(
        check_name=CHECK_NAME,
        passed=blocked_count == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"{blocked_count} of {total_count} pair(s) in {variant} blocked -- missing genuine "
            "per-country data for local_equity and/or the rate-based drivers."
        ),
        metadata={"blocked_count": blocked_count, "total_count": total_count},
    )

    yield Output(
        report,
        metadata={
            "variant": variant,
            "pair_count": total_count,
            "blocked_count": blocked_count,
            "preview": MetadataValue.md(report.to_markdown(index=False)),
        },
    )
