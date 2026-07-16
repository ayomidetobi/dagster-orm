"""Base infrastructure for DuckLake storage repositories."""

from __future__ import annotations

import structlog
import uuid
from contextlib import contextmanager
from typing import Iterator

import duckdb
import pandas as pd

from resources.duckdb_datacacher import SQL, render_ducklake_sql
from resources.duckdb_datacacher import quote_identifier as _quote_identifier


logger = structlog.get_logger(__name__)


class BaseDuckLakeRepository:
    """
    Base class for all DuckLake storage repositories.

    This class provides the common infrastructure required by concrete
    storage repositories while remaining completely domain agnostic.

    Responsibilities
    ----------------
    - Execute SQL
    - Manage transactions
    - Register temporary DataFrames
    - Qualify table names
    - Provide common logging and helper utilities

    It is intentionally unaware of business concepts such as metadata,
    values, snapshots, or validation.

    Notes
    -----
    The DuckDB connection is expected to be created and configured by
    ``DuckDBConnectionFactory``. This includes:

    - Extension installation
    - DuckLake initialization
    - S3 configuration
    - Catalog attachment

    Repositories never create or configure connections themselves.
    """

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> None:
        self._connection = connection
        self._catalog = catalog
        self._schema = schema
        self._logger = structlog.get_logger(self.__class__.__name__)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Execute a block inside a database transaction.

        The transaction is committed on success and rolled back
        automatically if an exception occurs.
        """
        self._connection.execute("BEGIN")

        try:
            yield
            self._connection.execute("COMMIT")
        except Exception:
            self._connection.execute("ROLLBACK")
            self._logger.exception("Transaction failed and was rolled back.")
            raise

    @contextmanager
    def register_dataframe(
        self,
        frame: pd.DataFrame,
        *,
        prefix: str = "tmp",
    ) -> Iterator[str]:
        """
        Register a pandas DataFrame as a temporary DuckDB relation.

        The relation is automatically unregistered when the context exits.

        Returns
        -------
        str
            Name of the temporary relation.
        """
        relation_name = f"{prefix}_{uuid.uuid4().hex}"

        self._connection.register(relation_name, frame)

        try:
            yield relation_name
        finally:
            try:
                self._connection.unregister(relation_name)
            except Exception:
                self._logger.warning(
                    "Unable to unregister temporary relation '%s'.",
                    relation_name,
                    exc_info=True,
                )

    def execute(
        self,
        sql: str | SQL,
        parameters: list | tuple | None = None,
    ) -> pd.DataFrame:
        """
        Execute SQL and return the result as a pandas DataFrame.
        """
        self._logger.debug("Executing SQL.")

        query = render_ducklake_sql(sql) if isinstance(sql, SQL) else sql

        if parameters is None:
            return self._connection.execute(query).fetchdf()

        return self._connection.execute(query, parameters).fetchdf()

    def execute_no_result(
        self,
        sql: str | SQL,
        parameters: list | tuple | None = None,
    ) -> None:
        """
        Execute SQL that does not return rows.
        """
        self._logger.debug("Executing SQL statement.")

        query = render_ducklake_sql(sql) if isinstance(sql, SQL) else sql

        if parameters is None:
            self._connection.execute(query)
        else:
            self._connection.execute(query, parameters)

    def qualify_table(self, table_name: str) -> str:
        """
        Return the fully qualified table name.

        Examples
        --------
        values

        market_data.values

        market_data.public.values
        """
        parts: list[str] = []

        if self._catalog:
            parts.append(self._catalog)

        if self._schema:
            parts.append(self._schema)

        parts.append(table_name)

        return ".".join(parts)

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """Safely quote a SQL identifier."""
        return _quote_identifier(identifier)
