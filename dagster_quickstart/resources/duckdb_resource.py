"""DuckDB database resource for Dagster.

This resource provides DuckDB database operations with S3 as the datalake.
Uses local DuckDBDataCacher for connection management.
"""

from contextlib import contextmanager
from typing import Iterator

import duckdb
from dagster import (
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
    get_dagster_logger,
)

from dagster_quickstart.resources.duckdb_datacacher import (
    DuckDBDataCacher,
)

logger = get_dagster_logger()


class DuckDBResource(ConfigurableResource):
    """Resource for interacting with a DuckDB database with S3 as the datalake.

    Uses duckdb_datacacher for connection management.
    Provides methods for querying, inserting data, and managing S3 Parquet files.
    Views are created dynamically over S3 control tables as needed.


    """

    cacher: ResourceDependency[DuckDBDataCacher]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._con = self.cacher.con

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Get the DuckDB connection.

        Returns:
            DuckDB connection object
        """
        yield self._con

    def get_bucket(self) -> str:
        """Get the S3 bucket name configured for this DuckDB resource.

        Returns:
            S3 bucket name string

        Raises:
            AttributeError: If bucket is not available in the cacher
        """
        if not hasattr(self.cacher, "bucket"):
            raise AttributeError("Bucket not available in duckdb_datacacher")
        return self.cacher.bucket
