"""Storage repository contracts for the rewrite package."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class MetadataStorageRepository(Protocol):
    """Storage contract for metadata and lookup data."""

    def get_metadata(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return metadata rows matching the requested filters."""

    def get_columns(self) -> list[str]:
        """Return the available column names, for filter validation/discovery."""

    def get_distinct_values(
        self,
        column: str,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
    ) -> list[str]:
        """Return the distinct, non-null values for a column, optionally filtered."""

    def save_metadata(self, frame: pd.DataFrame) -> None:
        """Persist normalized metadata rows."""

    def refresh_metadata(self) -> None:
        """Refresh any cached catalog state."""


@runtime_checkable
class ValueStorageRepository(Protocol):
    """Storage contract for value data."""

    def get_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        start: object | None = None,
        end: object | None = None,
        order_by: str | None = None,
        ascending: bool = True,
        limit: int | None = None,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return value rows for the requested series."""

    def get_last_values(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
        latest_non_null: bool = True,
        version: int | None = None,
        as_of: datetime | None = None,
    ) -> pd.DataFrame:
        """Return the latest value rows for the requested series."""

    def value_exists(
        self,
        series_codes: Sequence[str],
        *,
        ticker_source: str | None = None,
    ) -> Mapping[str, bool]:
        """Check whether values exist for the requested series."""

    def save_values(self, frame: pd.DataFrame) -> None:
        """Persist normalized value rows."""

    def delete_values(self, filters: Mapping[str, object]) -> None:
        """Delete value rows matching the supplied filters."""


@runtime_checkable
class LifecycleRepository(Protocol):
    """Lifecycle operations for initializing and maintaining storage."""

    def initialize_schema(self) -> None:
        """Create or migrate the managed schema."""

    def optimize(self) -> None:
        """Optimize managed storage structures."""

    def vacuum(self) -> None:
        """Reclaim storage space for managed tables."""


@runtime_checkable
class SnapshotRepository(Protocol):
    """Snapshot operations for versioned storage backends."""

    def list_snapshots(self) -> pd.DataFrame:
        """Return available snapshots."""

    def create_snapshot(self, label: str | None = None) -> str | None:
        """Create a new snapshot and return its identifier if available."""

    def restore_snapshot(self, snapshot_id: str) -> None:
        """Restore storage to the requested snapshot."""

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot if the backend supports it."""


@runtime_checkable
class StorageRepository(
    MetadataStorageRepository,
    ValueStorageRepository,
    LifecycleRepository,
    SnapshotRepository,
    Protocol,
):
    """Composite storage contract for implementations that support all capabilities."""


__all__ = [
    "LifecycleRepository",
    "MetadataStorageRepository",
    "SnapshotRepository",
    "StorageRepository",
    "ValueStorageRepository",
]
