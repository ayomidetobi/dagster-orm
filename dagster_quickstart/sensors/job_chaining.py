"""Reusable job-chaining sensor: start job B whenever job A reaches a given status.

Unlike sensors/derived_after_ingestion.py (bespoke -- waits for a *pair* of
jobs and tracks a cursor to fire once per pair), this is a generic
single-upstream -> single-downstream chain, usable for any job pair via
build_run_after_job_sensor(). Reach for the bespoke pattern when you need to
combine multiple upstream completions; reach for this when one job should
simply follow another.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Union

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    GraphDefinition,
    JobDefinition,
    RunRequest,
    RunStatusSensorContext,
    SensorDefinition,
    run_status_sensor,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

ChainableJob = Union[JobDefinition, GraphDefinition, UnresolvedAssetJobDefinition]

# context (context.partition_key is the upstream run's partition key, if
# any) -> the downstream partition key(s) to launch: one key, several (fans
# out to one RunRequest per key), or None to skip this trigger entirely.
PartitionMappingFn = Callable[[RunStatusSensorContext], Union[str, Sequence[str], None]]


def _resolve_downstream_partition_keys(
    context: RunStatusSensorContext,
    *,
    upstream_job_name: str,
    downstream_job_name: str,
    downstream_partitions_def: Any | None,
    partition_mapping_fn: PartitionMappingFn | None,
) -> list[str | None]:
    if downstream_partitions_def is None:
        return [None]

    if partition_mapping_fn is not None:
        mapped = partition_mapping_fn(context)
        if mapped is None:
            return []
        return [mapped] if isinstance(mapped, str) else list(mapped)

    # No mapping_fn given -- try the upstream run's own partition key
    # first (the common case: both jobs share the same partition scheme,
    # e.g. fetch_bloomberg_daily and compute_bloomberg_daily both keyed by
    # trade date), falling back to firing every downstream partition when
    # that key doesn't apply to the downstream scheme -- e.g. upstream is
    # unpartitioned (or date-keyed) but downstream is keyed by something
    # unrelated, like a fixed set of calculation types. This mirrors
    # sensors/derived_after_ingestion.py's
    # `for partition_key in DERIVED_CALC_PARTITIONS.get_partition_keys()`
    # fan-out for exactly that situation.
    upstream_partition_key = context.partition_key
    downstream_partition_keys = downstream_partitions_def.get_partition_keys()
    if upstream_partition_key is not None and upstream_partition_key in downstream_partition_keys:
        return [upstream_partition_key]

    if not downstream_partition_keys:
        raise ValueError(
            f"downstream job {downstream_job_name!r} is partitioned but has no partition "
            "keys to launch."
        )
    return list(downstream_partition_keys)


def build_run_after_job_sensor(
    *,
    upstream_job: ChainableJob,
    downstream_job: ChainableJob,
    name: str | None = None,
    run_status: DagsterRunStatus = DagsterRunStatus.SUCCESS,
    run_config: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    minimum_interval_seconds: int | None = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
    partition_mapping_fn: PartitionMappingFn | None = None,
) -> SensorDefinition:
    """Build a sensor that launches `downstream_job` whenever `upstream_job` reaches `run_status`.

    A thin, reusable wrapper around @run_status_sensor(monitored_jobs=...,
    request_job=...) -- call it once per job pair you want to chain:

        notify_after_bloomberg = build_run_after_job_sensor(
            upstream_job=fetch_bloomberg_daily_job,
            downstream_job=compute_bloomberg_daily_job,
        )

    run_status defaults to SUCCESS ("when job A finishes cleanly, start job
    B"); pass DagsterRunStatus.FAILURE to instead chain off a failure.
    run_config/tags are passed straight through to every downstream
    RunRequest. default_status is STOPPED so a newly built sensor doesn't
    start running the moment it's registered -- flip to RUNNING (or start
    it from the Dagster UI) once you've confirmed it's the pair you want.

    Partitions -- upstream and downstream don't have to share a partition
    scheme:

    - If the downstream job isn't partitioned, it just launches
      unpartitioned, regardless of the upstream run.
    - If both jobs share the same partition keys (e.g. fetch_bloomberg_daily
      and compute_bloomberg_daily both keyed by trade date), the upstream
      run's partition key carries over automatically -- an upstream run for
      "2024-06-01" launches downstream for "2024-06-01" too.
    - If the upstream run's partition key doesn't exist in the downstream
      scheme (including when upstream isn't partitioned at all), this fans
      out to EVERY downstream partition key by default -- e.g. an
      unpartitioned Bloomberg ingestion job completing fires one downstream
      run per entry in a StaticPartitionsDefinition of calc types. This is
      the same fan-out sensors/derived_after_ingestion.py performs by hand
      via `DERIVED_CALC_PARTITIONS.get_partition_keys()`.

    For anything more specific than that -- e.g. translating an upstream
    date key into a different downstream date format/offset, or firing only
    a subset of downstream partitions -- pass `partition_mapping_fn(context)
    -> str | Sequence[str] | None`:

        build_run_after_job_sensor(
            upstream_job=fetch_bloomberg_daily_job,
            downstream_job=compute_bloomberg_daily_job,
            partition_mapping_fn=lambda ctx: ctx.partition_key,  # explicit identity
        )

    Returning None skips the trigger for that run (no downstream RunRequest
    at all); a str or a list of strs launches one downstream run per key.
    """
    sensor_name = name or f"run_{downstream_job.name}_after_{upstream_job.name}"
    downstream_partitions_def = getattr(downstream_job, "partitions_def", None)

    @run_status_sensor(
        name=sensor_name,
        run_status=run_status,
        monitored_jobs=[upstream_job],
        request_job=downstream_job,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    )
    def _run_after_job_sensor(context: RunStatusSensorContext) -> list[RunRequest]:
        partition_keys = _resolve_downstream_partition_keys(
            context,
            upstream_job_name=upstream_job.name,
            downstream_job_name=downstream_job.name,
            downstream_partitions_def=downstream_partitions_def,
            partition_mapping_fn=partition_mapping_fn,
        )
        run_id = context.dagster_run.run_id
        return [
            RunRequest(
                run_key=f"{sensor_name}-{run_id}-{partition_key}" if partition_key else f"{sensor_name}-{run_id}",
                run_config=dict(run_config) if run_config else {},
                tags=dict(tags) if tags else None,
                partition_key=partition_key,
            )
            for partition_key in partition_keys
        ]

    return _run_after_job_sensor
