"""write_report/read_latest_report: persist/retrieve an availability report via DataAPI.

Writes to silver.fx_availability via data_api.write_table() -- the same generic, append-only
mechanism every other gold/silver table in this app uses (see
rewrite.data_api.repositories.generic_table_repository.GenericTableRepository). Append-only
means a re-run for the same variant doesn't overwrite, it adds a new snapshot on top -- so
every read goes through latest_snapshot() rather than a bare read_table(), and a caller that
forgot to filter would silently mix two runs' rows together.

A stale report is an acceptable tradeoff (driver coverage only changes when the catalog does),
but "we don't mind stale" has to mean *we can see how stale and decide*, not *we can't tell*:
read_latest_report() logs the report's age every time it's read, and raises rather than
returning empty when nothing has been written yet -- a silent empty read here would look like
"zero pairs available" instead of "nobody ran the availability check", which is a much worse
failure mode to debug.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

SILVER_SCHEMA = "silver"
FX_AVAILABILITY_TABLE = "fx_availability"


def latest_snapshot(frame: pd.DataFrame, *, as_of_column: str = "as_of") -> pd.DataFrame:
    """Every row from `frame`'s most recent `as_of_column` value.

    Generic to any append-only, as_of-stamped table -- not specific to fx_availability. Reading
    an append-only table without filtering to its latest as_of silently mixes two runs' rows
    together, a correctness bug rather than a staleness tradeoff. Empty in, empty out.
    """
    if frame.empty:
        return frame
    normalized = frame.copy()
    normalized[as_of_column] = pd.to_datetime(normalized[as_of_column])
    latest = normalized[as_of_column].max()
    return normalized[normalized[as_of_column] == latest]


def write_report(
    data_api: Any, report: pd.DataFrame, *, as_of: Optional[pd.Timestamp] = None
) -> None:
    """Append `report` (one build_availability_report() call's output) to silver.fx_availability.

    Tags every row with an `as_of` column (today, normalized to midnight, unless `as_of` is
    given) -- the column latest_snapshot()/read_latest_report() filter on. A no-op for an empty
    report (nothing to persist, and an empty frame has no columns to infer a table schema from).
    """
    if report.empty:
        return
    resolved_as_of = (
        pd.Timestamp(as_of)
        if as_of is not None
        else pd.Timestamp.utcnow().tz_localize(None).normalize()
    )
    tagged = report.copy()
    tagged.insert(0, "as_of", resolved_as_of)
    data_api.write_table(SILVER_SCHEMA, FX_AVAILABILITY_TABLE, tagged)


def read_latest_report(data_api: Any, variant: str) -> pd.DataFrame:
    """The most recent stored availability report for `variant`.

    Raises LookupError, naming the fix, if nothing has been written for this variant yet --
    never returns an empty frame silently (see module docstring for why that's the wrong
    failure mode here). Logs the report's age on every read -- visible, never gated on.
    """
    frame = data_api.read_table(SILVER_SCHEMA, FX_AVAILABILITY_TABLE, variant=variant).frame
    if frame.empty:
        raise LookupError(
            f"No availability report found for variant {variant!r} in "
            f"{SILVER_SCHEMA}.{FX_AVAILABILITY_TABLE} — run the fx_data_availability asset first."
        )

    latest = latest_snapshot(frame)
    as_of = pd.Timestamp(latest["as_of"].iloc[0])
    age_days = (pd.Timestamp.utcnow().tz_localize(None).normalize() - as_of).days
    logger.info(
        f"using availability report from {as_of.date()} ({age_days} days old)", variant=variant
    )
    return latest
