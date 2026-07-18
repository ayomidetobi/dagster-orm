"""Datetime utilities for consistent timezone-aware datetimes and parsing.

This module provides utilities for:
- Creating UTC timezone-aware datetimes
- Parsing timestamps with robust parsing (dateutil.parser.parse)
- Normalizing timestamps to match DateTime64 precision
- Normalizing pandas timestamp columns to UTC
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None  # type: ignore

# DateTime64 precision constants
# All DateTime64 columns use precision 6 (microseconds)
TIMESTAMP_PRECISION = 6  # microseconds

# UTC timezone
UTC = timezone.utc


def ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC timezone-aware.

    Args:
        dt: Datetime object (may be naive or timezone-aware)

    Returns:
        UTC timezone-aware datetime
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def normalize_timestamp_precision(dt: datetime, precision: int = TIMESTAMP_PRECISION) -> datetime:
    """Normalize timestamp to specified microsecond precision.

    Args:
        dt: Datetime object
        precision: Desired precision in microseconds (default: 6)

    Returns:
        Datetime with normalized microsecond precision
    """
    if precision >= 6:
        return dt

    divisor = 10 ** (6 - precision)
    return dt.replace(microsecond=(dt.microsecond // divisor) * divisor)


def parse_timestamp(
    timestamp: Any, default_timezone: Optional[timezone] = UTC
) -> Optional[datetime]:
    """Parse timestamp from various formats using dateutil.parser.parse.

    This function uses dateutil.parser.parse for robust parsing of various
    date/time formats. The result is always normalized to UTC.

    Args:
        timestamp: Timestamp in various formats:
            - datetime object (returned as-is, normalized to UTC)
            - str: ISO format, YYYY-MM-DD, YYYYMMDD, or other dateutil-parsable formats
            - int: YYYYMMDD format (8 digits)
        default_timezone: Timezone to assume for naive datetimes (default UTC)

    Returns:
        UTC timezone-aware datetime, or None if parsing fails

    Examples:
        >>> parse_timestamp("2024-01-15")
        datetime.datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
        >>> parse_timestamp("2024-01-15T10:30:00Z")
        datetime.datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> parse_timestamp(20240115)
        datetime.datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
    """
    if timestamp is None:
        return None

    # Already a datetime object
    if isinstance(timestamp, datetime):
        return normalize_timestamp_precision(ensure_utc(timestamp), TIMESTAMP_PRECISION)

    # Integer in YYYYMMDD format
    if isinstance(timestamp, int):
        date_str = str(timestamp)
        if len(date_str) == 8:  # YYYYMMDD format
            try:
                parsed = datetime.strptime(date_str, "%Y%m%d")
                return normalize_timestamp_precision(
                    parsed.replace(tzinfo=default_timezone), TIMESTAMP_PRECISION
                )
            except ValueError:
                return None

    # String - use dateutil.parser.parse for robust parsing
    if isinstance(timestamp, str):
        if dateutil_parser is None:
            # Fallback to basic parsing if dateutil not available
            try:
                # Try ISO format first
                parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=default_timezone)
                return normalize_timestamp_precision(ensure_utc(parsed), TIMESTAMP_PRECISION)
            except (ValueError, AttributeError):
                return None

        try:
            # dateutil.parser.parse can handle many formats including:
            # - ISO format: "2024-01-15T10:30:00Z"
            # - Date only: "2024-01-15"
            # - Various other formats
            parsed = dateutil_parser.parse(timestamp, default=datetime.now(UTC))
            # If parsed datetime is naive, use default_timezone
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=default_timezone)
            return normalize_timestamp_precision(ensure_utc(parsed), TIMESTAMP_PRECISION)
        except (ValueError, TypeError, OverflowError):
            # Parsing failed, return None
            return None

    return None


def utc_now() -> datetime:
    """Get current UTC datetime.

    Returns:
        Current UTC timezone-aware datetime
    """
    return datetime.now(UTC)


def parse_datetime_string(datetime_string: str) -> datetime:
    """Parse datetime string to UTC timezone-aware datetime.

    This is a convenience wrapper around parse_timestamp that ensures
    a datetime is returned (raises ValueError if parsing fails).

    Args:
        datetime_string: Datetime string in various formats (ISO, YYYY-MM-DD, etc.)

    Returns:
        UTC timezone-aware datetime

    Raises:
        ValueError: If datetime_string cannot be parsed
    """
    parsed = parse_timestamp(datetime_string)
    if parsed is None:
        raise ValueError(f"Could not parse datetime string: {datetime_string}")
    return parsed


def normalize_pandas_timestamp_to_utc(df: "pd.DataFrame", timestamp_column: str) -> "pd.DataFrame":
    """Normalize pandas DataFrame timestamp column to UTC timezone-aware datetime.

    Handles various timestamp formats:
    - Non-datetime columns: converts to datetime with UTC
    - Naive datetime columns: converts to UTC
    - Timezone-aware columns: converts to UTC

    Args:
        df: DataFrame with timestamp column to normalize
        timestamp_column: Name of timestamp column

    Returns:
        DataFrame with UTC-normalized timestamp column

    Raises:
        ImportError: If pandas is not available
    """
    if pd is None:
        raise ImportError("pandas is required for normalize_pandas_timestamp_to_utc")

    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    elif df[timestamp_column].dt.tz is None:
        df[timestamp_column] = pd.to_datetime(df[timestamp_column], utc=True)
    else:
        df[timestamp_column] = df[timestamp_column].dt.tz_convert("UTC")
    return df


def normalize_date_to_utc(date_value: Any) -> datetime:
    """Normalize date value to UTC datetime (date only, time set to 00:00:00).

    Args:
        date_value: Date value (datetime, date string, or date object)

    Returns:
        UTC datetime normalized to date (time set to 00:00:00)
    """
    if pd is None:
        raise ImportError("pandas is required for normalize_date_to_utc")

    return pd.to_datetime(date_value, utc=True).normalize()


def utc_midnight(dt: datetime) -> datetime:
    """Normalize to UTC midnight (00:00:00)."""
    return ensure_utc(dt).replace(hour=0, minute=0, second=0, microsecond=0)


def utc_today_midnight() -> datetime:
    """UTC midnight for the current calendar day."""
    return utc_midnight(utc_now())


def utc_yesterday_midnight() -> datetime:
    """UTC midnight for the previous calendar day."""
    return utc_today_midnight() - timedelta(days=1)


def utc_calendar_days_inclusive(start_date: datetime, end_date: datetime) -> List[datetime]:
    """UTC midnight for each calendar day from start through end (inclusive)."""
    start = utc_midnight(start_date)
    end = utc_midnight(end_date)
    out: List[datetime] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def iter_year_months(start_date: Any, end_date: Any) -> List[Tuple[int, int]]:
    """Inclusive calendar (year, month) pairs from start through end (UTC-normalized)."""
    if pd is None:
        raise ImportError("pandas is required for iter_year_months")
    start = normalize_date_to_utc(start_date)
    end = normalize_date_to_utc(end_date)
    y, m = int(start.year), int(start.month)
    ey, em = int(end.year), int(end.month)
    result: List[Tuple[int, int]] = []
    while (y, m) <= (ey, em):
        result.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return result


def dates_by_year_month(start_date: Any, end_date: Any) -> Dict[Tuple[int, int], List[datetime]]:
    """Map (year, month) to UTC midnights in [start_date, end_date] for that month."""
    if pd is None:
        raise ImportError("pandas is required for dates_by_year_month")
    start_ts = normalize_date_to_utc(start_date)
    end_ts = normalize_date_to_utc(end_date)
    dr = pd.date_range(start=start_ts, end=end_ts, freq="D", tz="UTC")
    buckets: Dict[Tuple[int, int], List[datetime]] = {}
    for ts in dr:
        dt = ts.to_pydatetime()
        key = (int(dt.year), int(dt.month))
        buckets.setdefault(key, []).append(dt)
    return buckets
