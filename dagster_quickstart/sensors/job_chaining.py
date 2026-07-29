"""Reusable job-chaining sensor: start job B whenever job A reaches a given status.

Unlike sensors/derived_after_ingestion.py (bespoke -- waits for a *pair* of
jobs and tracks a cursor to fire once per pair), this is a generic
single-upstream -> single-downstream chain, usable for any job pair via
build_run_after_job_sensor(). Reach for the bespoke pattern when you need to
combine multiple upstream completions; reach for this when one job should
simply follow another.
"""

from __future__ import annotations

from collections.abc import Mapping
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
) -> SensorDefinition:
    """Build a sensor that launches `downstream_job` whenever `upstream_job` reaches `run_status`.

    A thin, reusable wrapper around @run_status_sensor(monitored_jobs=...,
    request_job=...) -- call it once per job pair you want to chain:

        notify_after_bloomberg = build_run_after_job_sensor(
            upstream_job=bloomberg_daily_ingestion_job,
            downstream_job=calculate_derived_series_job,
        )

    run_status defaults to SUCCESS ("when job A finishes cleanly, start job
    B"); pass DagsterRunStatus.FAILURE to instead chain off a failure.
    run_config/tags are passed straight through to the downstream RunRequest.
    default_status is STOPPED so a newly built sensor doesn't start running
    the moment it's registered -- flip to RUNNING (or start it from the
    Dagster UI) once you've confirmed it's the pair you want.
    """
    sensor_name = name or f"run_{downstream_job.name}_after_{upstream_job.name}"

    @run_status_sensor(
        name=sensor_name,
        run_status=run_status,
        monitored_jobs=[upstream_job],
        request_job=downstream_job,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    )
    def _run_after_job_sensor(context: RunStatusSensorContext) -> RunRequest:
        return RunRequest(
            run_key=f"{sensor_name}-{context.dagster_run.run_id}",
            run_config=dict(run_config) if run_config else {},
            tags=dict(tags) if tags else None,
        )

    return _run_after_job_sensor
