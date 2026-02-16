"""PyPDL helper functions for Bloomberg data ingestion."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from dagster import AssetExecutionContext

from dagster_quickstart.orm.schema import DataPoint
from dagster_quickstart.utils.exceptions import PyPDLError

if TYPE_CHECKING:
    from dagster_quickstart.resources import PyPDLResource


def build_pypdl_request_params(
    field_name: str,
    tickers: List[str],
    start_date: datetime,
    end_date: datetime,
) -> Tuple[str, List[str], datetime, datetime]:
    """Build PyPDL request parameters for multiple tickers.

    Args:
        field_name: Field type name (should be Bloomberg field code like "PX_LAST")
        tickers: List of ticker symbols (List[str])
        start_date: Start date for ingestion
        end_date: End date for ingestion

    Returns:
        Tuple of (data_source, data_codes, start_date, end_date)
    """
    data_source = f"bloomberg/ts/{field_name}"
    data_codes = list(tickers)

    return data_source, data_codes, start_date, end_date


def fetch_bloomberg_data(
    pypdl_resource: "PyPDLResource",  # type: ignore[name-defined]
    data_codes: List[str],
    data_source: str,
    start_date: datetime,
    end_date: datetime,
    series_codes: Optional[List[str]] = None,
    context: Optional[AssetExecutionContext] = None,
    use_dummy_data: bool = False,
) -> Tuple[Optional[Dict[str, List[DataPoint]]], Optional[str]]:
    """Fetch data from Bloomberg via PyPDL for multiple series.

    Always processes multiple series and returns a dictionary mapping data_code to data points.

    Args:
        pypdl_resource: PyPDL resource
        data_codes: List of data codes (tickers) as List[str]
        data_source: Data source path
        start_date: Start date for data fetch
        end_date: End date for data fetch
        series_codes: Series codes for logging (List[str] or None)
        context: Dagster execution context for logging (optional)
        use_dummy_data: If True, return dummy/random data instead of calling API

    Returns:
        Tuple of (data_points, error_reason)
        data_points is Dict[str, List[DataPoint]] mapping data_code to list of data points
        If error_reason is not None, data_points will be None
    """
    if use_dummy_data:
        import random

        if context:
            context.log.info(
                "Fetching data from Bloomberg via PyPDL (DUMMY MODE - returning random data)",
                extra={
                    "series_codes": series_codes,
                    "data_codes": data_codes,
                    "data_source": data_source,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )

        data_by_code: Dict[str, List[DataPoint]] = {}
        for code in data_codes:
            data_points: List[DataPoint] = []
            current_date = start_date
            while current_date <= end_date:
                value = round(random.uniform(100.0, 200.0), 6)
                data_points.append({"timestamp": current_date, "value": value})
                current_date += timedelta(days=1)
            data_by_code[code] = data_points

        if context:
            context.log.info(
                "PyPDL fetch completed (DUMMY MODE)",
                extra={"data_point_counts": {k: len(v) for k, v in data_by_code.items()}},
            )
        return data_by_code, None

    try:
        if context:
            context.log.info(
                "Fetching data from Bloomberg via PyPDL",
                extra={
                    "series_codes": series_codes,
                    "data_codes": data_codes,
                    "data_source": data_source,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )

        results = pypdl_resource.fetch_time_series(
            data_codes=data_codes,
            data_source=data_source,
            start_date=start_date,
            end_date=end_date,
        )

        if context:
            context.log.info(
                "PyPDL fetch completed",
                extra={
                    "data_point_counts": {code: len(points) for code, points in results.items()}
                },
            )
        return results, None

    except PyPDLError as e:
        if context:
            context.log.error(
                "PyPDL fetch failed",
                extra={"error": str(e), "data_codes": data_codes, "series_codes": series_codes},
            )
        return None, str(e)
