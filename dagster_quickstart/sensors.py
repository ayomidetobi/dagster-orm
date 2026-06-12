"""Dagster sensor definitions.

Coordinates downstream jobs after upstream ingestion completes.
"""

import json
from datetime import datetime, timedelta
from typing import Iterator, Optional, Union

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from dagster_quickstart.assets.derived.partitions import DERIVED_CALC_PARTITIONS
from dagster_quickstart.jobs import (
    bloomberg_daily_ingestion_job,
    calculate_derived_series_job,
    hawk_daily_ingestion_job,
)

_BLOOMBERG_DAILY_JOB = bloomberg_daily_ingestion_job.name
_HAWK_DAILY_JOB = hawk_daily_ingestion_job.name


def _latest_successful_completion(
    context: SensorEvaluationContext,
    job_name: str,
) -> Optional[datetime]:
    """Return the end time of the most recent successful run for ``job_name``."""
    run_records = context.instance.get_run_records(
        filters=RunsFilter(job_name=job_name, statuses=[DagsterRunStatus.SUCCESS]),
        limit=1,
        order_by="update_timestamp",
        ascending=False,
    )
    if not run_records:
        return None

    end_time = run_records[0].end_time
    if end_time is None:
        return None
    return datetime.fromtimestamp(end_time)


def _parse_cursor_timestamp(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    return None


def _ingestion_pair_ready_for_derived_calc(
    bloomberg_completion: datetime,
    hawk_completion: datetime,
    previous_cursor: dict,
    one_day_ago: datetime,
) -> bool:
    previous_bloomberg = _parse_cursor_timestamp(previous_cursor.get("bloomberg_completion"))
    previous_hawk = _parse_cursor_timestamp(previous_cursor.get("hawk_completion"))
    both_recent = bloomberg_completion > one_day_ago and hawk_completion > one_day_ago
    bloomberg_is_new = previous_bloomberg is None or bloomberg_completion > previous_bloomberg
    hawk_is_new = previous_hawk is None or hawk_completion > previous_hawk
    return both_recent and bloomberg_is_new and hawk_is_new


@sensor(
    job=calculate_derived_series_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def derived_after_ingestion_sensor(
    context: SensorEvaluationContext,
) -> Iterator[Union[RunRequest, SkipReason]]:
    """Trigger derived-series calculation after Bloomberg and Hawk daily ingestion succeed.

    Uses a cursor to fire at most once per pair of upstream completions.
    """
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)

    bloomberg_completion = _latest_successful_completion(context, _BLOOMBERG_DAILY_JOB)
    hawk_completion = _latest_successful_completion(context, _HAWK_DAILY_JOB)

    if bloomberg_completion is None or hawk_completion is None:
        yield SkipReason("Waiting for Bloomberg and Hawk daily ingestion to complete.")
        return

    previous_cursor = json.loads(context.cursor) if context.cursor else {}

    if not _ingestion_pair_ready_for_derived_calc(
        bloomberg_completion,
        hawk_completion,
        previous_cursor,
        one_day_ago,
    ):
        yield SkipReason(
            "Bloomberg and Hawk daily ingestion have not both completed since the last trigger."
        )
        return

    context.update_cursor(
        json.dumps(
            {
                "bloomberg_completion": bloomberg_completion.timestamp(),
                "hawk_completion": hawk_completion.timestamp(),
            }
        )
    )

    run_key_suffix = (
        f"{int(bloomberg_completion.timestamp())}-{int(hawk_completion.timestamp())}"
    )
    for partition_key in DERIVED_CALC_PARTITIONS.get_partition_keys():
        yield RunRequest(
            run_key=f"derived-{partition_key}-{run_key_suffix}",
            partition_key=partition_key,
        )
