"""Shared DuckLake snapshot/time-travel SQL helper."""

from __future__ import annotations

from datetime import datetime

import structlog

from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.rewrite.data_api.errors import SnapshotConflictError

logger = structlog.get_logger(__name__)


def table_reference(
    table_name: str,
    *,
    version: int | None = None,
    as_of: datetime | None = None,
) -> SQL:
    """Build a FROM target, pinned to a DuckLake snapshot if requested."""

    if version is not None and as_of is not None:
        logger.warning("snapshot_conflict", table=table_name, version=version, as_of=as_of)
        raise SnapshotConflictError("Specify only one of version or as_of.")

    if version is not None:
        return SQL(
            "$table AT (VERSION => $version)",
            table=SQL.identifier(table_name),
            version=version,
        )

    if as_of is not None:
        return SQL(
            "$table AT (TIMESTAMP => $as_of)",
            table=SQL.identifier(table_name),
            as_of=as_of,
        )

    return SQL("$table", table=SQL.identifier(table_name))
