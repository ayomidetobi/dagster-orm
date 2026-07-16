"""Domain-specific exceptions for the rewrite data API.

Every raise site in rewrite/data_api/ should use one of these (or a new,
equally specific subclass of RewriteError) instead of a bare
ValueError/RuntimeError, so callers can catch failures by what actually went
wrong rather than by accident of implementation.
"""

from __future__ import annotations


class RewriteError(Exception):
    """Base class for all rewrite/data_api domain errors."""


class FrameValidationError(RewriteError):
    """Raised when a DataFrame fails pandera schema validation."""


class SeriesCodesRequiredError(RewriteError):
    """Raised when an operation is called with no series codes."""


class UnsupportedFileTypeError(RewriteError):
    """Raised when a file extension isn't supported for ingestion."""


class InvalidOrderByError(RewriteError):
    """Raised when a requested order_by column isn't present in the result."""


class MissingMetadataColumnError(RewriteError):
    """Raised when metadata is missing a column required for a vendor fetch."""


class UnsupportedTickerSourceError(RewriteError):
    """Raised when a ticker_source has no configured vendor/column mapping."""


class UnsupportedVendorError(RewriteError):
    """Raised when a vendor name has no registered VendorClient."""


class UnknownCalcTypeError(RewriteError):
    """Raised when a derived series references an unregistered calc_type."""


class InvalidParentSeriesCountError(RewriteError):
    """Raised when a calc_type's parent series count doesn't match its arity."""


class SnapshotConflictError(RewriteError):
    """Raised when both version and as_of are requested for the same query."""


class TickerSourceRequiredError(RewriteError):
    """Raised when an out_of_cache/live query is missing a ticker_source."""


class DirectFetchUnavailableError(RewriteError):
    """Raised when a live() query has no DirectFetchService configured."""


class DuckLakeConfigError(RewriteError):
    """Raised when DuckLake catalog configuration is incomplete."""


class InvalidQueryError(RewriteError):
    """Raised when value-query parameters fail validation."""


class InvalidFilterFieldError(RewriteError):
    """Raised when a metadata filter references a column that doesn't exist.

    The message lists the available columns so the caller can pick a valid
    one, rather than surfacing a raw SQL "column not found" error.
    """


class InvalidFilterValueError(RewriteError):
    """Raised in strict mode when a metadata filter value doesn't exist in the data.

    In non-strict mode, the same mismatch is logged as a warning and the
    invalid value is dropped rather than raised.
    """
