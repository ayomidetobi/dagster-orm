"""DuckLake-native Bloomberg values ingestion asset (rewrite DataAPI).

Gets Bloomberg-sourced metadata, fetches live values straight from the
vendor, and writes them into the DuckLake values table -- no wide-format
monthly partitions, no S3 control tables. See assets/ingestion/bloomberg/
for the legacy orm-based wide-partition asset. Data quality is validated
in-process via a check_spec (see check.py's build_values_quality_check_result),
directly against the values_df get_values() returns -- no second DuckLake query.
"""

from datetime import datetime

from dagster import AssetCheckSpec, AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_quickstart.assets.ingestion.bloomberg_rewrite.check import (
    CHECK_NAME,
    build_values_quality_check_result,
)
from dagster_quickstart.assets.ingestion.bloomberg_rewrite.config import BloombergValuesConfig
from dagster_quickstart.rewrite.data_api.columns import MetadataColumns, TickerSource
from dagster_quickstart.rewrite.data_api.vendors.ticker_columns import resolve_ticker_field_columns

BBG_TICKER_COLUMN, _ = resolve_ticker_field_columns(TickerSource.BLOOMBERG)


@asset(
    required_resource_keys={"rewrite_data_api"},
    name="ingest_bloomberg_values",
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NAME,
            asset="ingest_bloomberg_values",
            description=(
                "Validates fetched Bloomberg values with pandera -- no series with "
                "zero data, no non-finite values, unique/non-future timestamps"
            ),
        )
    ],
)
def ingest_bloomberg_values(context: AssetExecutionContext, config: BloombergValuesConfig):
    """Fetch Bloomberg values live and write them into the DuckLake values table.

    Args:
        context: Dagster asset execution context
        config: BloombergValuesConfig with series/date-range selection

    Yields:
        AssetCheckResult for validate_bloomberg_values_quality, then a
        MaterializeResult with series/row counts and the S3 path the values
        were written to (queried live from DuckLake, not assumed).
    """
    data_api = context.resources.rewrite_data_api.api

    metadata_df = data_api.get_metadata().frame
    if BBG_TICKER_COLUMN in metadata_df.columns:
        bloomberg_metadata = metadata_df[metadata_df[BBG_TICKER_COLUMN].notna()]
    else:
        bloomberg_metadata = metadata_df.iloc[0:0]

    if config.series_codes:
        bloomberg_metadata = bloomberg_metadata[
            bloomberg_metadata[MetadataColumns.SERIES_CODE].isin(config.series_codes)
        ]

    series_codes = (
        bloomberg_metadata[MetadataColumns.SERIES_CODE]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if not series_codes:
        context.log.warning("No Bloomberg-sourced series found in metadata")
        yield build_values_quality_check_result(None, log=context.log)
        yield MaterializeResult(metadata={"series_count": 0, "timestamp_count": 0})
        return

    context.log.info(f"Fetching {len(series_codes)} Bloomberg series live")

    values_df = data_api.get_values(
        series_codes,
        ticker_source=TickerSource.BLOOMBERG,
        out_of_cache=True,
        start=datetime.fromisoformat(config.start) if config.start else None,
        end=datetime.fromisoformat(config.end) if config.end else None,
    )

    if values_df.empty:
        context.log.warning("Bloomberg vendor returned no values for the requested series")
        yield build_values_quality_check_result(values_df, log=context.log)
        yield MaterializeResult(
            metadata={"series_count": len(series_codes), "timestamp_count": 0}
        )
        return

    data_api.write_values(values_df)

    data_points_written = int(values_df.notna().sum().sum())


    context.log.info(
        f"Wrote {data_points_written} Bloomberg data point(s) across "
    )

    yield build_values_quality_check_result(values_df, log=context.log)

    yield MaterializeResult(
        metadata={
            "series_count": len(values_df.columns),
            "timestamp_count": len(values_df),
            "data_points_written": data_points_written,
            "preview": MetadataValue.md(values_df.head(10).to_markdown()),
            "series_codes_sample": MetadataValue.json(series_codes[:20]),

        }
    )
