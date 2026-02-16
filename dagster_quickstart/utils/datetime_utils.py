"""Datetime utilities for consistent timezone-aware datetimes and parsing.

This module provides utilities for:
- Creating UTC timezone-aware datetimes
- Parsing timestamps with robust parsing (dateutil.parser.parse)
- Normalizing timestamps to match DateTime64 precision
"""

from datetime import datetime, timezone
from typing import Any, Optional

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
