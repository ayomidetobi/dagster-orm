"""PyPDL data ingestion resource using pyeqdr.pypdl library."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from dagster import ConfigurableResource, get_dagster_logger

from dagster_quickstart.utils.datetime_utils import parse_timestamp
from dagster_quickstart.utils.exceptions import PyPDLError, PyPDLExecutionError

try:
    import pyeqdr.pypdl as pypdl
except ImportError:
    pypdl = None  # type: ignore

logger = get_dagster_logger()


class PyPDLResource(ConfigurableResource):
    """PyPDL API resource using pyeqdr.pypdl library.

    This resource uses PyPDL to fetch time-series data from Bloomberg.
    Dagster handles concurrency through dynamic partitions.

    Retries and error handling are managed by Dagster's retry policies
    at the asset level, not within this resource.
    """

    def fetch_time_series(
        self,
        data_code: Union[str, List[str]],
        data_source: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        """Fetch time-series data for single or multiple series.

        Uses bulk logic for both single and multiple series.
        For single series: returns List[Dict[str, Any]]
        For multiple series: returns Dict[str, List[Dict[str, Any]]] mapping data_code to data points

        Args:
            data_code: The ticker/code (str) or list of tickers/codes (List[str])
            data_source: The data source path (e.g., "bloomberg/ts/PX_LAST")
            start_date: Start date for data retrieval
            end_date: End date for data retrieval

        Returns:
            For single: List of data point dicts with timestamp and value
            For multiple: Dict mapping data_code to list of data point dicts

        Raises:
            PyPDLError: If PyPDL is not installed or execution fails
            PyPDLExecutionError: If PyPDL execution fails
        """
        if pypdl is None:
            raise PyPDLError(
                "pyeqdr.pypdl is not installed. Please install it with: pip install pyeqdr"
            )

        # Convert to list if single (bulk logic handles both)
        is_single = isinstance(data_code, str)
        if is_single:
            # Type narrowing: we know data_code is str here
            single_code: str = data_code
            data_codes: List[str] = [single_code]
        else:
            data_codes = list(data_code)

        if not data_codes:
            return [] if is_single else {}

        try:
            # Always use bulk logic (works for single and multiple)
            pdl_dict, wrapper_program_name, result_keys = _build_pdl_dict_bulk(
                data_codes, data_source, start_date, end_date
            )

            logger.info(
                f"Executing PyPDL program for {len(data_codes)} series ({data_source})",
                extra={
                    "data_codes": data_codes,
                    "data_source": data_source,
                    "count": len(data_codes),
                },
            )

            # Execute the wrapper program
            out_d = pypdl.gnp_exec(
                pdl_dict,
                wrapper_program_name,
                self.host,
                self.port,
                self.username,
            )

            # Process results with error handling from wrapper program
            results: Dict[str, List[Dict[str, Any]]] = {}
            _process_pypdl_result_bulk(out_d, data_codes, data_source, result_keys, results)

            logger.info(
                f"Fetched data for {len(results)} series",
                extra={
                    "data_codes": data_codes,
                    "result_counts": {k: len(v) for k, v in results.items()},
                },
            )

            # Return single list if input was single, otherwise return dict
            if is_single:
                return results[data_codes[0]]
            return results

        except Exception as e:
            logger.error(
                f"PyPDL execution failed: {e}",
                extra={"data_codes": data_codes, "data_source": data_source, "error": str(e)},
            )
            raise PyPDLExecutionError("PyPDL execution failed") from e


def _build_pdl_dict_bulk(
    data_codes: List[str],
    data_source: str,
    start_date: datetime,
    end_date: datetime,
) -> Tuple[Dict[str, Any], str, List[str]]:
    """Build PyPDL dictionary for multiple series using wrapper program pattern.

    Creates individual py_get_time_serie objects and wraps them in a parent program
    that executes all of them via exec_list.

    Returns:
        Tuple of:
            - pdl_dict: PyPDL program dictionary
            - wrapper_program_name: Name of the wrapper program ("r")
            - result_keys: list of result key prefixes (one per series)
    """
    pdl_dict: Dict[str, Any] = {}
    exec_list: List[str] = []
    result_keys: List[str] = []

    date_1_str = start_date.strftime("%Y%m%d")
    date_2_str = end_date.strftime("%Y%m%d")

    # Create individual py_get_time_serie objects
    for idx, code in enumerate(data_codes):
        name = f"r{idx}"
        result = f"z{idx}"

        pypdl.pdl_make_object(
            pdl_dict,
            name,
            "py_get_time_serie",
            {
                "date_1": pypdl.to_date(date_1_str),
                "date_2": pypdl.to_date(date_2_str),
                "data_source": data_source,
                "data_code": code,
                "result": result,
            },
        )

        exec_list.append(name)
        result_keys.append(result)

    # Create wrapper program that executes all individual programs
    wrapper_program_name = "r"
    pypdl.pdl_make_object(
        pdl_dict,
        wrapper_program_name,
        "program",
        {
            "exec_list": exec_list,
            "result": "z",
        },
    )

    return pdl_dict, wrapper_program_name, result_keys


def _process_pypdl_result_bulk(
    out_d: Dict[str, Any],
    data_codes: List[str],
    data_source: str,
    result_keys: List[str],
    results: Dict[str, List[Dict[str, Any]]],
) -> List[str]:
    """Process PyPDL bulk execution result with wrapper program error handling.

    Checks for errors in the wrapper program's error_index_list and removes
    failed results, then processes valid results.

    Args:
        out_d: PyPDL output dictionary
        data_codes: List of data codes (tickers) - will be modified to remove failed ones
        data_source: Data source path
        result_keys: List of result key prefixes (z0, z1, z2, etc.) - will be modified to remove failed ones
        results: Dictionary to populate with results (data_code -> data points)

    Returns:
        List of valid result keys that were successfully processed
    """
    # Check for errors in wrapper program result ("z.error_index_list")
    error_index_list = out_d.get("z.error_index_list", [])

    # Log errors and remove failed results (error_index_list is 1-based)
    if error_index_list:
        # Sort in reverse order to remove from end to beginning (preserves indices)
        error_indices = sorted(map(int, error_index_list), reverse=True)
        exec_list = [
            f"r{i}" for i in range(len(data_codes))
        ]  # Reconstruct exec_list for error messages

        for error_idx in error_indices:
            # error_idx is 1-based, convert to 0-based
            idx = error_idx - 1
            if 0 <= idx < len(data_codes):
                data_code = data_codes[idx]

                # Get error stack
                error_stack_key = f"z.error_stack_{error_idx}"
                error_stack_list = out_d.get(error_stack_key, [])
                error_stack = (
                    ". ".join(str(item) for item in error_stack_list) if error_stack_list else ""
                )

                logger.error(
                    f"PyPDL request failed for {data_code}",
                    extra={
                        "data_code": data_code,
                        "data_source": data_source,
                        "error_stack": error_stack,
                        "request": exec_list[idx] if idx < len(exec_list) else "unknown",
                    },
                )

                # Remove from lists (in reverse order, so indices remain valid)
                result_keys.pop(idx)
                data_codes.pop(idx)

    # Process valid results
    for data_code, result_key in zip(data_codes, result_keys):
        date_list = out_d.get(f"{result_key}.date_list", [])
        value_list = out_d.get(f"{result_key}.value_list", [])

        if not date_list or not value_list:
            logger.warning(
                f"No data for {data_code} ({data_source})",
                extra={"data_code": data_code, "data_source": data_source},
            )
            results[data_code] = []
            continue

        data_points = _convert_to_data_points(date_list, value_list, data_code)
        if data_points:
            logger.info(
                f"Fetched {len(data_points)} data points for {data_code}",
                extra={"data_code": data_code, "data_point_count": len(data_points)},
            )
        results[data_code] = data_points

    return result_keys


def _process_pypdl_result_single(
    out_d: Dict[str, Any],
    data_code: str,
    data_source: str,
    result_key: str,
) -> List[Dict[str, Any]]:
    """Process PyPDL execution result and extract data points for a single series.

    Args:
        out_d: PyPDL output dictionary
        data_code: Data code for the series
        data_source: Data source for the series
        result_key: Result key prefix

    Returns:
        List of data point dicts with timestamp and value
    """
    # Check for errors
    error_index_list = out_d.get(f"{result_key}.error_index_list")
    if error_index_list:
        error_stack_key = f"{result_key}.error_stack_1"
        error_stack_list = out_d.get(error_stack_key, [])
        error_stack = ", ".join(str(item) for item in error_stack_list) if error_stack_list else ""
        logger.error(
            f"PyPDL request failed for {data_code}",
            extra={"data_code": data_code, "data_source": data_source, "error_stack": error_stack},
        )
        raise PyPDLExecutionError(f"PyPDL request failed for {data_code}: {error_stack}")

    # Get data
    date_list = out_d.get(f"{result_key}.date_list", [])
    value_list = out_d.get(f"{result_key}.value_list", [])

    if not date_list or not value_list:
        logger.warning(
            f"No data for {data_code} ({data_source})",
            extra={"data_code": data_code, "data_source": data_source},
        )
        return []

    data_points = _convert_to_data_points(date_list, value_list, data_code)
    if data_points:
        logger.info(
            f"Fetched {len(data_points)} data points for {data_code}",
            extra={"data_code": data_code, "data_point_count": len(data_points)},
        )

    return data_points


def _convert_to_data_points(
    date_list: List[Any], value_list: List[Any], data_code: str
) -> List[Dict[str, Any]]:
    """Convert PyPDL date and value lists to data points.

    Args:
        date_list: List of dates from PyPDL
        value_list: List of values from PyPDL
        data_code: Data code for logging

    Returns:
        List of data point dicts with timestamp and value
    """
    data_points: List[Dict[str, Any]] = []

    for dt, spot in zip(date_list, value_list):
        timestamp = _parse_pypdl_date(dt)
        if timestamp is None:
            continue

        try:
            value = float(spot)
            if value != value:  # Check for NaN
                continue
            data_points.append({"timestamp": timestamp, "value": value})
        except (ValueError, TypeError):
            logger.warning(
                f"Could not convert value to float for {data_code}: {spot}",
                extra={"data_code": data_code, "value": str(spot)},
            )
            continue

    return data_points


def _parse_pypdl_date(dt: Any) -> Optional[datetime]:
    """Parse PyPDL date to UTC timezone-aware datetime.

    Uses dateutil.parser.parse for robust parsing and normalizes to UTC
    with DateTime64(6) precision (microseconds).

    Args:
        dt: Date from PyPDL (int, datetime, or str)

    Returns:
        UTC timezone-aware datetime normalized to microsecond precision, or None if parsing fails
    """
    # Try using the centralized parse_timestamp utility which handles:
    # - datetime objects (normalizes to UTC)
    # - int in YYYYMMDD format
    # - strings (uses dateutil.parser.parse for robust parsing)
    parsed = parse_timestamp(dt)
    if parsed is not None:
        return parsed

    # Fallback: Try using pypdl.date_to_string for int dates
    if isinstance(dt, int):
        try:
            date_str = pypdl.date_to_string(dt, short=1)
            parsed = parse_timestamp(date_str)
            if parsed is not None:
                return parsed
        except Exception:
            pass

    logger.warning(
        f"Could not parse PyPDL date: {dt}",
        extra={"date_value": str(dt), "date_type": type(dt).__name__},
    )
    return None
