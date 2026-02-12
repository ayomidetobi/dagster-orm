"""Domain-specific exceptions for the ORM layer."""


class MetadataResolutionError(Exception):
    """Raised when metadata query fails to resolve series codes."""

    pass


class SeriesNotFoundError(Exception):
    """Raised when no series are found matching the filter criteria."""

    pass


class InvalidFilterFieldError(Exception):
    """Raised when a filter field is not a valid metadata column."""

    pass


class ValueQueryParameterError(Exception):
    """Raised when value query parameters are invalid."""

    pass


class ConnectionBindingError(Exception):
    """Raised when connection is not properly bound to the ORM."""

    pass


class InvalidQueryError(Exception):
    """Raised when a query execution fails."""

    pass
