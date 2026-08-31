"""Exceptions for the steer/ package, mirroring rewrite/data_api/errors.py's style."""

from __future__ import annotations


class SteerError(Exception):
    """Base class for every steer/ exception."""


class InsufficientDataError(SteerError):
    """Not enough history in the requested window to fit/test anything meaningful."""
