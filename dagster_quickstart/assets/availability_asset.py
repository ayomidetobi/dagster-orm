"""fx_data_availability: report of which pairs in this variant are blocked, and why.

Moved out of assets/steer/ (and out of the "steer" asset group -- no group_name here, so this
lands in Dagster's default group) since availability resolution is generic
(dagster_quickstart.availability), not STEER-specific -- see that package's docstring.
STEER's role/variant vocabulary comes from steer.config.STEER_AVAILABILITY_SPEC, passed
explicitly to build_availability_report()/discover_pairs() below; this asset itself doesn't
hardcode any of STEER's answers.

One variant partition (G10/EM/CHN, same static scheme as the rest of the
STEER graph -- see assets/steer/partitions.py) covers every pair in that
variant in one materialization. The check FAILS (WARN, not ERROR -- a data
completeness gap, not a broken pipeline) whenever any pair is blocked, so
the gap is visible in the Dagster UI rather than only in this asset's own
metadata table.

The report includes a flat `{leg}_{role}` column per resolved driver role
(e.g. base_swap_2y, quote_local_equity -- see
dagster_quickstart.availability.report.build_availability_report), not
just the blocked/reason summary -- steer_silver_prices reconstructs each
pair's PairAvailability straight from these columns via
PairAvailability.from_report_row.

Writes the report to silver.fx_availability (dagster_quickstart.availability.storage.write_report)
rather than yielding it as a Dagster Output -- no downstream asset takes it as an input, no
`deps=` either. steer_silver_prices (and Steer.fit(), for the script/notebook path) each read
the stored report independently via read_latest_report(); the two are no longer connected in
the Dagster graph at all, so this asset and steer_silver_prices can run on different schedules,
different runs, or out of order -- read_latest_report() logs how stale the report it found was,
rather than either asset silently assuming freshness.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC

CHECK_NAME = "no_blocked_pairs"

#: Derived from STEER_AVAILABILITY_SPEC.variants, not imported from
#: assets/steer/partitions.py's STEER_PARTITIONS -- this asset lives outside assets/steer/ now
#: (see module docstring), so importing that module back would leak the package boundary this
#: move is meant to establish. tests/test_availability_asset_partitions.py asserts the two stay
#: identical (silver reads the stored report keyed by variant, so a mismatch would silently
#: return nothing) -- asserted, not assumed.
FX_AVAILABILITY_PARTITIONS = StaticPartitionsDefinition(list(STEER_AVAILABILITY_SPEC.variants))


@asset(
    name="fx_data_availability",
    partitions_def=FX_AVAILABILITY_PARTITIONS,
    required_resource_keys={"rewrite_data_api"},
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="fx_data_availability",
            description="Fails (WARN) if any pair in this variant is missing genuine per-country driver data.",
        )
    ],
)
def fx_data_availability(context: AssetExecutionContext):
    """Build the data_availability report for every pair in this variant.

    See dagster_quickstart.availability's package docstring for exactly
    what "blocked" means and why local_equity/rate-differential aren't
    substituted with a global proxy when missing.
    """
    from dagster_quickstart.availability.report import build_availability_report
    from dagster_quickstart.availability.storage import write_report
    from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC
    from dagster_quickstart.steer.source.discovery import discover_pairs

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
        yield MaterializeResult(metadata={"pair_count": 0})
        return

    report = build_availability_report({variant: pairs}, data_api, STEER_AVAILABILITY_SPEC)

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

    write_report(data_api, report)

    yield MaterializeResult(
        metadata={
            "variant": variant,
            "pair_count": total_count,
            "blocked_count": blocked_count,
            "preview": MetadataValue.md(report.to_markdown(index=False)),
        },
    )
