"""The silver pipeline: fetch every pair's raw rate + driver series for a universe and conform them.

Pure Python -- no Dagster -- so it's directly callable from a script or a
test with a stub data_api, not just from assets/steer/silver_asset.py's
@asset wrapper. That wrapper reduces to reading the partition key/resources,
calling build_silver_frame(), and mapping SilverResult onto Output/
AssetCheckResult -- every domain decision (which series a universe's pairs
need, how a blocked/stale pair is skipped, how a pair's frame is conformed)
lives here instead.

assess_freshness lives here too (not a separate assets/steer/ module) --
"is this pair's bronze data current enough to use" is exactly the kind of
domain rule steer/ owns; FRESHNESS_TOLERANCE_DAYS is model-owner-tunable
the same way a StrategyConfig field would be, it just isn't per-universe
today. Only FRESHNESS_CHECK_NAME (the Dagster AssetCheckSpec's name) stays
in assets/steer/freshness_check.py.

Logging: this module returns structured results (SilverResult) rather than
logging anything itself -- it has no context.log to call, and threading a
logger in would leak the Dagster shape back across the steer/ boundary.
skipped_reasons carries exactly what the asset used to log inline (one
entry per blocked pair, "blocked: <reasons>"), in the same wording, so the
asset can still log line-for-line what it did before.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from dagster_quickstart.steer.config import StrategyConfig
from dagster_quickstart.steer.constants import RATE_COLUMN, SERIES_CODE_COLUMN, UNIVERSE_CHN
from dagster_quickstart.steer.discovery import PairAvailability
from dagster_quickstart.steer.features import (
    DriverValues,
    fetch_raw_driver_frame,
    required_series_codes,
    resolve_flows_cutover,
)
from dagster_quickstart.steer.silver import conform_to_business_days

#: A pair is "fresh" if its rate series has a value within this many days of
#: `as_of` -- generous enough to tolerate a weekend/holiday without
#: false-failing every Monday, but tight enough to catch upstream ingestion
#: actually having stopped.
FRESHNESS_TOLERANCE_DAYS = 3


def assess_freshness(
    raw: Optional[pd.DataFrame],
    *,
    as_of: pd.Timestamp,
    tolerance_days: int = FRESHNESS_TOLERANCE_DAYS,
) -> Tuple[bool, str]:
    """(is_fresh, reason) for one pair's raw frame.

    raw=None or empty means nothing was fetched at all -- always stale
    (there's no bronze data for this pair yet).
    """
    if raw is None or raw.empty:
        return False, "no bronze data"

    latest_timestamp = raw.index.max()
    age_days = (as_of - latest_timestamp).days
    if age_days <= tolerance_days:
        return True, f"{age_days}d old"
    return False, f"{age_days}d old (tolerance {tolerance_days}d)"


@dataclass(frozen=True)
class SilverResult:
    """Everything the silver asset needs to emit, computed without Dagster.

    blocked_pairs/stale_pairs are the series_code (and, for stale, "CODE
    (reason)") lists the asset's AssetCheckResult/Output metadata already
    reports. skipped_reasons is series_code -> "blocked: <reasons>" for
    every BLOCKED pair only (matching exactly what steer_silver_prices used
    to log inline via context.log.info -- stale pairs were never logged as
    individual lines, only counted in the check, so they're not in here).
    chn_flows_cutover_error is set instead of logging a warning directly,
    when this is a CHN universe and resolve_flows_cutover() failed.
    """

    frame: pd.DataFrame
    pair_count: int
    fetched_pair_count: int
    blocked_pairs: List[str] = field(default_factory=list)
    stale_pairs: List[str] = field(default_factory=list)
    skipped_reasons: Dict[str, str] = field(default_factory=dict)
    chn_flows_cutover_error: Optional[str] = None


def build_silver_frame(
    data_api: Any,
    universe: str,
    strategy_config: StrategyConfig,
    availabilities: Sequence[PairAvailability],
    *,
    as_of: pd.Timestamp,
) -> SilverResult:
    """Fetch every pair's rate + drivers and conform them onto a business-day calendar.

    `availabilities` is one universe's PairAvailability per pair (e.g. from
    steer.discovery.pairs_from_availability_report). A pair with
    availability.blocked (missing genuine per-country data for local_equity
    or the rate-based drivers -- see steer/discovery.py's module docstring)
    is skipped and never fetched further; a pair whose bronze data isn't
    fresh as of `as_of` (see assess_freshness) is skipped too. Skipping one
    pair never raises -- the caller decides what to do with an empty result.

    Values are fetched ONCE for the whole universe, not once per pair (see
    steer.features.DriverValues) -- required_series_codes() collects every
    pair's needed series upfront (including blocked pairs -- cheap, and
    simpler than tracking which ones to skip) and DriverValues.load()
    fetches them all in one call; fetch_raw_driver_frame then only slices
    columns out of that already-loaded frame, per pair, in memory.
    """
    all_series_codes = required_series_codes(
        ((availability.series_code, availability) for availability in availabilities),
        strategy_config,
    )
    driver_values = DriverValues.load(data_api, all_series_codes)

    chn_flows_cutover = None
    chn_flows_cutover_error = None
    if universe == UNIVERSE_CHN:
        try:
            chn_flows_cutover = resolve_flows_cutover(data_api)
        except ValueError as exc:
            # Resolved once, upfront, rather than per pair -- but tolerantly: a metadata
            # hiccup here shouldn't fail every OTHER CHN driver too. fetch_raw_driver_frame
            # raises its own clear error later, only if a pair's flows data actually needs it.
            chn_flows_cutover_error = str(exc)

    conformed_frames = []
    blocked_pairs: List[str] = []
    stale_pairs: List[str] = []
    skipped_reasons: Dict[str, str] = {}

    for availability in availabilities:
        if availability.blocked:
            blocked_pairs.append(availability.series_code)
            skipped_reasons[availability.series_code] = f"blocked: {'; '.join(availability.block_reasons)}"
            continue

        raw = fetch_raw_driver_frame(
            driver_values,
            availability.series_code,
            strategy_config,
            availability,
            chn_flows_cutover=chn_flows_cutover,
        )
        is_fresh, reason = assess_freshness(raw, as_of=as_of)
        if not is_fresh:
            stale_pairs.append(f"{availability.series_code} ({reason})")
            continue

        conformed = conform_to_business_days(raw, primary_column=RATE_COLUMN).copy()
        conformed[SERIES_CODE_COLUMN] = availability.series_code
        conformed_frames.append(conformed)

    combined = pd.concat(conformed_frames) if conformed_frames else pd.DataFrame()

    return SilverResult(
        frame=combined,
        pair_count=len(availabilities),
        fetched_pair_count=len(conformed_frames),
        blocked_pairs=blocked_pairs,
        stale_pairs=stale_pairs,
        skipped_reasons=skipped_reasons,
        chn_flows_cutover_error=chn_flows_cutover_error,
    )
