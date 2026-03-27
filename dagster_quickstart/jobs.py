"""Dagster jobs configuration.

Defines asset jobs with configuration loaded from YAML files.
"""

import os

from dagster import config_from_files, define_asset_job, file_relative_path

from dagster_quickstart.assets import (
    calculate_derived_series,
    ingest_bloomberg_data_backfill,
    ingest_bloomberg_data_daily,
    load_lookup_tables_to_s3,
    load_meta_series_to_s3,
    load_series_dependencies_to_s3,
)

# Job for loading control tables (lookup, meta_series, metadata_derived)
load_control_tables_job = define_asset_job(
    name="load_control_tables_job",
    selection=[
        load_lookup_tables_to_s3,
        load_meta_series_to_s3,
        load_series_dependencies_to_s3,
    ],
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "load_control_tables.yaml"),
            )
        ]
    ),
)

# Job for Bloomberg daily ingestion
bloomberg_daily_ingestion_job = define_asset_job(
    name="bloomberg_daily_ingestion_job",
    selection=[ingest_bloomberg_data_daily],
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "bloomberg_daily.yaml"),
            )
        ]
    ),
)

# Job for Bloomberg backfill ingestion
bloomberg_backfill_ingestion_job = define_asset_job(
    name="bloomberg_backfill_ingestion_job",
    selection=[ingest_bloomberg_data_backfill],
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "bloomberg_backfill.yaml"),
            )
        ]
    ),
)

# Job for calculating derived series
calculate_derived_series_job = define_asset_job(
    name="calculate_derived_series_job",
    selection=[calculate_derived_series],
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "derived_series.yaml"),
            )
        ]
    ),
)

# Bloomberg field partitions (PX_LAST, …) differ from derived calc partitions (SPREAD, …);
# they cannot share one partitioned job. Run ``calculate_derived_series_job`` per derived partition
# after ingestion (e.g. backfill all keys in ``DERIVED_CALC_PARTITIONS``).
populate_value_data_job = define_asset_job(
    name="populate_value_data_job",
    selection=[ingest_bloomberg_data_daily],
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "populate_value_data.yaml"),
            )
        ]
    ),
)

# Job for all assets - automatically follows dependency graph.
# Note: ``ingest_bloomberg_data_daily`` and ``calculate_derived_series`` use different
# ``partitions_def``; materializing this job may require partition-specific runs in the UI.
all_assets_job = define_asset_job(
    name="all_assets_job",
    # No selection specified - includes all assets and respects dependency graph
    config=config_from_files(
        [
            file_relative_path(
                __file__,
                os.path.join("run_config", "pipeline.yaml"),
            )
        ]
    ),
)
