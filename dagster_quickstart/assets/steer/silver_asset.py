"""Silver layer: fetch every pair's raw rate + driver series for this universe and conform them.

One partition per universe (G10/EM/CHN, static -- see partitions.py) --
this asset loops over every currency_pair (series_code) discovered in that
universe and fetches/conforms each one's full history, concatenating into
one long-form frame tagged by a `series_code` column. currency_pair is
data here, not a Dagster partition.

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

from dagster_quickstart.assets.steer._shared import resolve_universe_pairs
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
def steer_silver_prices(context: AssetExecutionContext):
    """Fetch this universe's every pair's rate + drivers from DuckLake and conform them.

    Reads bronze (rewrite_data_api's DuckLake `values` table -- see
    steer.features.fetch_raw_driver_frame) via each pair's own series_code
    (the rate) plus this universe's configured global_equity_series/
    commodity_series and any discovered rate-differential series, then
    aligns everything onto one business-day calendar with limited
    forward-fill for real holidays/gaps (see
    steer.silver.conform_to_business_days).

    A pair steer.discovery marks blocked (missing genuine per-country data
    for local_equity or the rate-based drivers) is skipped -- see
    steer/discovery.py's module docstring. A pair with stale bronze data
    is also skipped, and counted in the freshness check.
    """
    from dagster_quickstart.steer.discovery import (
        build_currency_to_equity_series,
        build_currency_to_fi_series,
    )
    from dagster_quickstart.steer.features import fetch_raw_driver_frame
    from dagster_quickstart.steer.silver import conform_to_business_days

    universe = context.partition_key
    data_api = context.resources.rewrite_data_api.api
    strategy_config = context.resources.steer_config.for_universe(universe)

    fixed_income_metadata = data_api.get_metadata(asset_class=["Fixed Income"]).frame
    currency_to_fi_series = build_currency_to_fi_series(fixed_income_metadata)

    equity_metadata = data_api.get_metadata(asset_class=["Equity"]).frame
    currency_to_equity_series = build_currency_to_equity_series(equity_metadata)

    pairs = resolve_universe_pairs(
        universe,
        data_api,
        currency_to_fi_series=currency_to_fi_series,
        currency_to_equity_series=currency_to_equity_series,
    )
    as_of = pd.Timestamp.utcnow().tz_localize(None).normalize()

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

        raw = fetch_raw_driver_frame(data_api, pair.series_code, strategy_config, pair.availability)
        is_fresh, reason = assess_freshness(raw, as_of=as_of)
        if not is_fresh:
            stale_pairs.append(f"{pair.series_code} ({reason})")
            continue

        conformed = conform_to_business_days(raw).copy()
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
