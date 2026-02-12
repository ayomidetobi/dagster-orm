"""Custom exceptions for dagster_quickstart utilities."""


class PyPDLError(Exception):
    """Raised when PyPDL operations fail."""

    pass


class PyPDLExecutionError(PyPDLError):
    """Raised when PyPDL execution fails."""

    pass
