"""Legacy DuckDB datacacher for S3 Parquet operations (qr_common-compatible).

Plain S3-Parquet caching -- save()/load() queries against S3 via DuckDB's
httpfs extension. No DuckLake awareness at all; see ducklake_datacacher.py
for attaching a DuckLake catalog instead.
"""

from __future__ import annotations

import json
import warnings
from datetime import datetime
from string import Template
from typing import Any, Dict, Optional

import duckdb
import pandas as pd
import structlog

from .config import S3SecretConfig
from .utils import (
    SQL,
    collect_dataframes,
    create_s3_secret,
    install_windows_plugins,
    is_stale,
    join_s3,
    load_extension,
)

logger = structlog.get_logger(__name__)


class DuckDBDataCacher:
    """DuckDB datacacher for S3 Parquet operations.

    Provides methods for saving/loading data to/from S3 using DuckDB's httpfs extension.
    Handles SQL query resolution with placeholder bindings.
    """

    def __init__(
        self,
        app_config: Optional[Dict[str, Any]] = None,
        bucket: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        env_name: Optional[str] = None,
        pandas_analyze_sample: Optional[int] = None,
        app_name: Optional[str] = None,
    ):
        """Initialize DuckDBDataCacher with S3 credentials.

        Can be initialized either with:
        1. app_config dict (legacy format from qr_common)
        2. Direct S3 credentials (bucket, access_key, secret_key, region)

        Args:
            app_config: Application configuration dict with S3_KEY (legacy format)
            bucket: S3 bucket name (if not using app_config)
            access_key: S3 access key (if not using app_config)
            secret_key: S3 secret key (if not using app_config)
            region: S3 region (if not using app_config)
            env_name: Environment name (default: 'dev' if using app_config)
            pandas_analyze_sample: Unused, kept for compatibility
            app_name: Unused, kept for compatibility
        """
        # Load S3 credentials
        if app_config:
            # Legacy format: load from app_config
            env_name = env_name or app_config.get("env", "dev")
            s3_data = app_config.get("S3_KEY", [None])[0]
            if s3_data:
                s_creds = json.loads(s3_data) if isinstance(s3_data, str) else s3_data
                env_creds = s_creds[env_name]
                self.bucket = env_creds["bucket"]
                access_key = env_creds["access_key"]
                secret_key = env_creds["secret_key"]
                region = env_creds["region"]
            else:
                raise ValueError("S3_KEY not found in app_config")
        else:
            # Direct credentials
            if not all([bucket, access_key, secret_key, region]):
                raise ValueError(
                    "Either app_config or all of (bucket, access_key, secret_key, region) must be provided"
                )
            self.bucket = bucket

        install_windows_plugins()

        self._con = duckdb.connect(database=":memory:")
        load_extension(self._con, "httpfs")
        create_s3_secret(
            self._con,
            S3SecretConfig(key_id=access_key, secret=secret_key, region=region),
        )

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection.

        Returns:
            DuckDB connection object
        """
        return self._con

    # ------------------------------------------------------------------
    # SQL → STRING RESOLUTION
    # ------------------------------------------------------------------
    def _sql_to_string(self, s: SQL) -> str:
        """Replace SQL placeholders with bound values.

        Example:
            SQL("select * from $file_path", file_path="data.parquet")

        Returns:
            select * from s3://BUCKET/data.parquet

        Args:
            s: SQL object with query and bindings

        Returns:
            Resolved SQL string with all placeholders replaced

        Raises:
            ValueError: If s is not a SQL object
        """
        if not isinstance(s, SQL):
            raise ValueError(f"Expected SQL object, got {type(s)}")

        replacements: Dict[str, str] = {}

        if "file_path" not in s.bindings:
            warnings.warn("If this is a SELECT query from load(), 'file_path' is missing.")

        for key, binding_value in s.bindings.items():
            # Resolve S3 path
            if key == "file_path":
                resolved_value = join_s3(self.bucket, binding_value)
            # Handle encryption credentials
            elif key == "credentials":
                # TODO: decrypt before storing
                self._con.execute(f"PRAGMA add_parquet_key('key256', '{binding_value}');")
                resolved_value = "key256"
            # Pandas DataFrame
            elif isinstance(binding_value, pd.DataFrame):
                relation = f"df_{id(binding_value)}"
                self._con.register(relation, binding_value)
                resolved_value = relation
            # Nested SQL
            elif isinstance(binding_value, SQL):
                resolved_value = f"({self._sql_to_string(binding_value)})"
            # Primitive types
            elif isinstance(binding_value, (int, float, bool)):
                resolved_value = str(binding_value)
            elif isinstance(binding_value, str):
                resolved_value = binding_value
            elif binding_value is None:
                resolved_value = "null"
            else:
                raise ValueError(f"Invalid type for SQL binding '{key}': {type(binding_value)}")

            replacements[key] = resolved_value

        return Template(s.sql).safe_substitute(replacements)

    # ------------------------------------------------------------------
    # SAVE TO S3 (PARQUET)
    # ------------------------------------------------------------------
    def save(
        self,
        select_statement: SQL,
        file_path: str,
        debug: bool = False,
        credentials: Optional[str] = None,
    ) -> bool:
        """Save query results to S3 as Parquet file.

        Args:
            select_statement: SQL object with query and bindings
            file_path: Relative S3 file path (relative to bucket)
            debug: If True, log the generated query
            credentials: Optional encryption credentials for Parquet file

        Returns:
            True if save was successful

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        if select_statement is None:
            raise ValueError("select_statement is None")

        if not isinstance(select_statement, SQL):
            raise ValueError(f"Expected SQL; got {type(select_statement)}")

        # Register DataFrames used in SQL
        dataframes = collect_dataframes(select_statement)
        for key, value in dataframes.items():
            self._con.register(key, value)

        # Optional Parquet encryption
        if credentials is not None:
            self._con.execute(f"PRAGMA add_parquet_key('key256', '{credentials}');")

        url = join_s3(self.bucket, file_path)

        query = self._sql_to_string(
            SQL(
                """
                COPY $select_statement
                TO '$url'
                (FORMAT PARQUET)
                """,
                select_statement=select_statement,
                url=url,
            )
        )

        if debug:
            logger.info(f"QUERY: {query}")

        self._con.execute(query)
        return True

    # ------------------------------------------------------------------
    # LOAD FROM S3
    # ------------------------------------------------------------------
    def load(
        self,
        select_statement: SQL,
        debug: bool = False,
    ) -> Optional[pd.DataFrame]:
        """Load data from S3 Parquet file into a Pandas DataFrame.

        Args:
            select_statement: SQL object with query and bindings
            debug: If True, log the generated query

        Returns:
            Pandas DataFrame with loaded data, or None if result is empty

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        if select_statement is None:
            raise ValueError("select_statement is None")

        if not isinstance(select_statement, SQL):
            raise ValueError(f"Expected SQL; got {type(select_statement)}")

        query = self._sql_to_string(select_statement)

        if debug:
            logger.info(f"QUERY: {query}")

        result = self._con.execute(query)

        if result is None:
            return None

        df = result.df()
        return None if df.empty else df

    # ------------------------------------------------------------------
    # STALENESS CHECK
    # ------------------------------------------------------------------
    def staleness_check(
        self,
        file_name: str,
        lookback_delta_seconds: int,
        in_memory: bool = False,
    ) -> bool:
        """Check whether a file or in-memory table is stale.

        Args:
            file_name: Name of the file or table to check
            lookback_delta_seconds: Maximum age in seconds before considered stale
            in_memory: If True, check in-memory table (recommended for DuckDB)

        Returns:
            True if stale, False if fresh or unknown
        """
        if not in_memory:
            warnings.warn("This functionality is best suited for in-memory tables.")

        try:
            result = self._con.execute(
                f"SELECT last_modified FROM pragma_storage_info('{file_name}')"
            ).fetchone()
        except Exception as exc:
            logger.error(f"Failed to fetch last modified info: {exc}")
            return False

        if not result or result[0] is None:
            logger.error("No last modified data available.")
            return False

        return is_stale(
            datetime.fromtimestamp(result[0]),
            lookback_delta_seconds,
            label="File last modified at",
        )


def duckdb_datacacher(
    app_config: Optional[Dict[str, Any]] = None,
    bucket: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: Optional[str] = None,
    env_name: Optional[str] = None,
) -> DuckDBDataCacher:
    """Factory function to create DuckDBDataCacher instance.

    This function provides a compatible interface with the qr_common version.

    Args:
        app_config: Application configuration dict with S3_KEY (legacy format)
        bucket: S3 bucket name (if not using app_config)
        access_key: S3 access key (if not using app_config)
        secret_key: S3 secret key (if not using app_config)
        region: S3 region (if not using app_config, e.g., 'us-east-1', 'eu-north-1')
        env_name: Environment name (default: 'dev' if using app_config)

    Returns:
        DuckDBDataCacher instance

    Example:
        # Using direct credentials
        cacher = duckdb_datacacher(
            bucket="my-bucket",
            access_key="AKIA...",
            secret_key="secret...",
            region="us-east-1"
        )

        # Using app_config (legacy)
        cacher = duckdb_datacacher(app_config=config)
    """
    return DuckDBDataCacher(
        app_config=app_config,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        env_name=env_name,
    )
