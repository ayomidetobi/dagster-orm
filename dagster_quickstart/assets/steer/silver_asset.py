"""Silver layer: fetch every pair's raw rate + driver series for this universe and conform them.

One partition per universe (G10/EM/CHN, static -- see partitions.py) --
this asset loops over every currency_pair (series_code) discovered in that
universe and fetches/conforms each one's full history, concatenating into
one long-form frame tagged by a `series_code` column. currency_pair is
data here, not a Dagster partition.

Depends on steer_data_availability (same partitions_def, so Dagster aligns
the partition and the IO manager hands this asset the upstream frame
directly -- no manual S3/Parquet read) instead of re-discovering pairs and
re-resolving every driver role itself: the two assets used to
independently redo the exact same (role, currency) resolution work for the
same partition, which is what made this asset's runtime track
steer_data_availability's runtime plus one get_values() call. Every pair's
PairAvailability is reconstructed straight from the report's `{leg}_{role}`
columns (see _shared.pairs_from_availability_report) -- this asset issues
zero get_metadata() calls for role resolution.

Values are fetched ONCE for the whole partition, not once per pair --
G10's 45 pairs (say) share USD's 4 role series and the 2 global-driver
series heavily, so 45 separate get_values() calls would each refetch
mostly-overlapping data. steer.features.required_series_codes() collects
every pair's needed series upfront (including blocked pairs -- cheap, and
simpler than tracking which ones to skip) and steer.features.DriverValues.load()
fetches them all in one call; fetch_raw_driver_frame then only slices
columns out of that already-loaded frame, per pair, in memory.

Also the gate for per-pair blocking: a pair missing genuine per-country
data for local_equity or the rate-based drivers is skipped here (logged,
counted, and never fetched further) -- never passed through to estimation
on a partial/corrupted input (see steer/discovery.py's module docstring
for why). Skipping one pair never fails the partition -- see the
aggregate freshness/availability check below.

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

from dagster_quickstart.assets.steer._shared import pairs_from_availability_report
from dagster_quickstart.assets.steer.freshness_check import FRESHNESS_CHECK_NAME, assess_freshness
from dagster_quickstart.assets.steer.partitions import STEER_PARTITIONS

SERIES_CODE_COLUMN = "series_code"


@asset(
    name="steer_silver_prices",
    partitions_def=STEER_PARTITIONS,
    required_resource_keys={"rewrite_data_api", "steer_config"},
    check_specs=[
        AssetCheckSpec(
            name=FRESHNESS_CHECK_NAME,
            asset="steer_silver_prices",
            description="Fails (WARN) if any pair in this universe isn't fresh as of the run date.",
        )
    ],
    group_name="steer",
)
def steer_silver_prices(context: AssetExecutionContext, steer_data_availability: pd.DataFrame):
    """Fetch this universe's every pair's rate + drivers from DuckLake and conform them.

    Reads bronze (rewrite_data_api's DuckLake `values` table -- see
    steer.features.fetch_raw_driver_frame) via each pair's own series_code
    (the rate) plus this universe's configured global_equity_series/
    commodity_series and any discovered rate-differential series, then
    aligns everything onto one business-day calendar with limited
    forward-fill for real holidays/gaps (see
    steer.silver.conform_to_business_days).

    A pair steer_data_availability marks blocked (missing genuine
    per-country data for local_equity or the rate-based drivers) is
    skipped -- see steer/discovery.py's module docstring. A pair with
    stale bronze data is also skipped, and counted in the freshness check.
    """
    from dagster_quickstart.steer.features import (
        RATE_COLUMN,
        DriverValues,
        fetch_raw_driver_frame,
        required_series_codes,
        resolve_flows_cutover,
    )
    from dagster_quickstart.steer.silver import conform_to_business_days

    universe = context.partition_key
    data_api = context.resources.rewrite_data_api.api
    strategy_config = context.resources.steer_config.for_universe(universe)

    pairs = pairs_from_availability_report(steer_data_availability)
    as_of = pd.Timestamp.utcnow().tz_localize(None).normalize()

    # One get_values() call for the whole partition -- every pair (blocked or not, since
    # required_series_codes() is cheap and simpler than tracking which to skip) shares this
    # single wide frame instead of issuing its own fetch. See module docstring.
    all_series_codes = required_series_codes(
        ((pair.series_code, pair.availability) for pair in pairs), strategy_config
    )
    driver_values = DriverValues.load(data_api, all_series_codes)

    chn_flows_cutover = None
    if universe == "CHN":
        try:
            chn_flows_cutover = resolve_flows_cutover(data_api)
        except ValueError as exc:
            # Resolved once, upfront, rather than per pair -- but tolerantly: a metadata
            # hiccup here shouldn't crash every OTHER CHN driver too. fetch_raw_driver_frame
            # raises its own clear error later, only if a pair's flows data actually needs it.
            context.log.warning(f"Could not resolve CHN flows cutover: {exc}")

    conformed_frames = []
    blocked_pairs = []
    stale_pairs = []

    for pair in pairs:
        if pair.availability.blocked:
            blocked_pairs.append(pair.series_code)
            context.log.info(
                f"Skipping {pair.series_code} -- blocked: {'; '.join(pair.availability.block_reasons)}"
            )
            continue

        raw = fetch_raw_driver_frame(
            driver_values,
            pair.series_code,
            strategy_config,
            pair.availability,
            chn_flows_cutover=chn_flows_cutover,
        )
        is_fresh, reason = assess_freshness(raw, as_of=as_of)
        if not is_fresh:
            stale_pairs.append(f"{pair.series_code} ({reason})")
            continue

        conformed = conform_to_business_days(raw, primary_column=RATE_COLUMN).copy()
        conformed[SERIES_CODE_COLUMN] = pair.series_code
        conformed_frames.append(conformed)

    combined = pd.concat(conformed_frames) if conformed_frames else pd.DataFrame()

    yield AssetCheckResult(
        check_name=FRESHNESS_CHECK_NAME,
        passed=len(stale_pairs) == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"{len(stale_pairs)} of {len(pairs)} pair(s) stale as of {as_of.date()}."
            + (f" Stale: {stale_pairs}" if stale_pairs else "")
        ),
        metadata={
            "pair_count": len(pairs),
            "stale_pairs": stale_pairs,
            "blocked_pairs": blocked_pairs,
        },
    )

    yield Output(
        combined,
        metadata={
            "universe": universe,
            "pair_count": len(pairs),
            "fetched_pair_count": len(conformed_frames),
            "blocked_pair_count": len(blocked_pairs),
            "stale_pair_count": len(stale_pairs),
            "row_count": len(combined),
            "preview": MetadataValue.md(combined.tail(10).to_markdown())
            if not combined.empty
            else MetadataValue.md("(no data available for this universe)"),
        },
    )
