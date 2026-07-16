"""DuckDB connection helpers for S3 and DuckLake."""

import glob
import json
import os
import sys
import warnings
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any, Dict, Iterable, Optional, Protocol

import duckdb
import pandas as pd
import structlog

from rewrite.data_api.errors import DuckLakeConfigError
from rewrite.data_api.models.config import (
    DuckLakeCatalogConfig,
    DuckLakeConfig,
    S3SecretConfig,
)

logger = structlog.get_logger(__name__)


class SQL:
    """SQL query object with placeholder bindings.

    Example:
        sql = SQL("SELECT * FROM $table WHERE id = $id", table="users", id=123)
        # Can be used with DuckDBDataCacher to resolve placeholders
    """

    def __init__(self, sql: str, **bindings: Any):
        """Initialize SQL object with query and bindings.

        Args:
            sql: SQL query string with $placeholder syntax
            **bindings: Key-value pairs for placeholder substitution
        """
        self.sql = sql
        self.bindings = bindings

    def __add__(self, other: "SQL") -> "SQL":
        """Concatenate two SQL fragments.

        Each side is rendered via render_ducklake_sql() using only its own
        bindings *before* concatenation, so fragments built with generically
        named placeholders (e.g. every WHERE clause using $column/$value)
        never collide when joined together.
        """
        if not isinstance(other, SQL):
            return NotImplemented
        return SQL(render_ducklake_sql(self) + render_ducklake_sql(other))

    @staticmethod
    def identifier(name: str) -> "SQLIdentifier":
        """Mark a binding value as a SQL identifier (quoted, not a literal)."""
        return SQLIdentifier(name)

    @staticmethod
    def join(parts: list["SQL"], separator: "SQL", *, prefix: str = "") -> "SQL":
        """Join SQL fragments with a separator, optionally prefixed."""
        if not parts:
            return SQL("")

        combined = parts[0]
        for part in parts[1:]:
            combined = combined + separator + part

        if prefix:
            combined = SQL(prefix) + combined

        return combined


@dataclass(frozen=True, slots=True)
class SQLIdentifier:
    """Marks a SQL binding value as an identifier (table/column name) rather than a literal."""

    name: str


def quote_identifier(identifier: str) -> str:
    """Safely quote a SQL identifier, splitting on '.' for catalog.schema.table names."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in identifier.split("."))


def sql_literal(value: str) -> str:
    """Return a safely quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _format_sql_literal(value: Any) -> str:
    """Render a scalar Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return sql_literal(value.isoformat())
    if isinstance(value, str):
        return sql_literal(value)
    raise ValueError(f"Cannot render SQL literal for type: {type(value)}")


def render_ducklake_sql(sql_obj: "SQL") -> str:
    """Render a SQL object into plain SQL text for DuckLake table queries.

    This is a pure, connection-free counterpart to
    DuckDBDataCacher._sql_to_string(): it has no notion of S3 file paths or
    Parquet encryption keys. Identifier bindings are quoted, scalars/tuples
    are rendered as SQL literals, and nested SQL objects render recursively.
    """
    if not isinstance(sql_obj, SQL):
        raise ValueError(f"Expected SQL object, got {type(sql_obj)}")

    replacements: Dict[str, str] = {}

    for key, value in sql_obj.bindings.items():
        if isinstance(value, SQLIdentifier):
            replacements[key] = quote_identifier(value.name)
        elif isinstance(value, SQL):
            replacements[key] = render_ducklake_sql(value)
        elif isinstance(value, (list, tuple)):
            replacements[key] = "(" + ", ".join(_format_sql_literal(v) for v in value) + ")"
        else:
            replacements[key] = _format_sql_literal(value)

    return Template(sql_obj.sql).safe_substitute(replacements)


def join_s3(bucket: str, relative_path: str) -> str:
    """Join bucket and relative path into full S3 URI.

    Returns S3 URI format (s3://bucket/path) for DuckDB's httpfs extension.
    DuckDB's httpfs handles S3 URIs directly without URL encoding.

    Args:
        bucket: S3 bucket name
        relative_path: Relative path within bucket (may contain = characters)

    Returns:
        Full S3 URI (e.g., 's3://bucket/control/lookup/version=2026-01-12/data.parquet')
        Note: Path is NOT URL-encoded - DuckDB's httpfs handles S3 URIs directly
    """
    # Remove leading slash from relative_path if present
    clean_path = relative_path.lstrip("/")
    # Return S3 URI format - DuckDB's httpfs extension handles this directly
    # Do NOT URL-encode the path - httpfs will handle S3 URIs properly
    return f"s3://{bucket}/{clean_path}"


def collect_dataframes(sql_obj: SQL) -> Dict[str, pd.DataFrame]:
    """Collect all pandas DataFrames from SQL bindings.

    Args:
        sql_obj: SQL object with bindings

    Returns:
        Dictionary mapping relation names to DataFrames
    """
    dataframes: Dict[str, pd.DataFrame] = {}

    for value in sql_obj.bindings.values():
        if isinstance(value, pd.DataFrame):
            relation = f"df_{id(value)}"
            dataframes[relation] = value
        elif isinstance(value, SQL):
            # Recursively collect from nested SQL
            nested_dfs = collect_dataframes(value)
            dataframes.update(nested_dfs)

    return dataframes


def install_plugin(plugin_name: str, extension_name: str) -> None:
    """Install DuckDB plugin (Windows-specific helper).

    Args:
        plugin_name: Name of the plugin file
        extension_name: Name of the extension
    """
    try:
        duckdb.install_extension(extension_name)
        duckdb.load_extension(extension_name)
        logger.info(f"Installed and loaded plugin: {extension_name}")
    except Exception as e:
        logger.error(f"Failed to install plugin {extension_name}: {e}")


class DuckLakeCatalogBackend(Protocol):
    """DuckLake catalog backend contract.

    Backends can be attached independently from extension loading.
    Each backend declares the DuckDB extensions it needs and how to attach
    its catalog to a live connection.
    """

    def required_extensions(self) -> tuple[str, ...]:
        """Return DuckDB extensions required by this backend."""
        ...

    def attach(self, con: duckdb.DuckDBPyConnection) -> None:
        """Attach the catalog to the provided DuckDB connection."""
        ...


@dataclass(frozen=True, slots=True)
class PostgresDuckLakeCatalogBackend:
    """DuckLake catalog backend that attaches a PostgreSQL catalog."""

    config: DuckLakeCatalogConfig
    ducklake_extension: DuckLakeConfig | None = None

    def required_extensions(self) -> tuple[str, ...]:
        extension = self.ducklake_extension or DuckLakeConfig()
        return (
            extension.extension_name,
            extension.postgres_extension_name,
        )

    def attach(self, con: duckdb.DuckDBPyConnection) -> None:
        catalog = self.config
        if catalog.postgres is None:
            raise DuckLakeConfigError("DuckLake catalog configuration requires PostgreSQL settings")

        pg = catalog.postgres
        target = (
            "ducklake:postgres:"
            f"dbname={pg.database} host={pg.host} port={pg.port} "
            f"user={pg.user} password={pg.password}"
        )
        if pg.sslmode:
            target += f" sslmode={pg.sslmode}"
        options: list[str] = [
            f"DATA_PATH {sql_literal(catalog.data_path)}",
            f"METADATA_SCHEMA {sql_literal(catalog.schema_name)}",
        ]
        if catalog.attach_options:
            options.extend(catalog.attach_options)
        con.execute(
            f"ATTACH {sql_literal(target)} AS {catalog.catalog_alias} ({', '.join(options)})"
        )
        try:
            con.execute(f"USE {catalog.catalog_alias}")
        except Exception:
            logger.warning("ducklake_use_failed", catalog_alias=catalog.catalog_alias)


@dataclass(frozen=True, slots=True)
class DuckDBConnectionFactory:
    """Create preconfigured DuckDB connections for S3 and DuckLake."""

    bucket: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: Optional[str] = None
    database: str = ":memory:"
    ducklake_extension: Optional[DuckLakeConfig] = None
    ducklake_catalog: Optional[DuckLakeCatalogConfig] = None
    ducklake_catalog_backend: DuckLakeCatalogBackend | None = None
    s3_secret: S3SecretConfig | None = None
    enable_ducklake: bool = False

    def _load_extension(self, con: duckdb.DuckDBPyConnection, extension_name: str) -> None:
        """Install and load a single DuckDB extension."""
        con.execute(f"INSTALL {extension_name}")
        con.execute(f"LOAD {extension_name}")

    def _load_extensions(
        self,
        con: duckdb.DuckDBPyConnection,
        extension_names: Iterable[str],
    ) -> None:
        """Install and load extensions once, preserving order."""
        seen: set[str] = set()
        for extension_name in extension_names:
            if extension_name in seen:
                continue
            seen.add(extension_name)
            self._load_extension(con, extension_name)

    def _load_httpfs(self, con: duckdb.DuckDBPyConnection) -> None:
        """Install and load httpfs for S3 access."""
        extension = self.ducklake_extension or DuckLakeConfig()
        self._load_extension(con, extension.httpfs_extension_name)

    def _create_s3_secret(
        self,
        con: duckdb.DuckDBPyConnection,
        secret: S3SecretConfig,
    ) -> None:
        """Create or replace the DuckDB S3 secret used by httpfs."""
        clauses = [
            "TYPE s3",
            f"PROVIDER {secret.provider}",
            f"KEY_ID {sql_literal(secret.key_id)}",
            f"SECRET {sql_literal(secret.secret)}",
            f"REGION {sql_literal(secret.region)}",
        ]
        if secret.session_token is not None:
            clauses.append(f"SESSION_TOKEN {sql_literal(secret.session_token)}")
        if secret.endpoint is not None:
            clauses.append(f"ENDPOINT {sql_literal(secret.endpoint)}")
        sql = f"CREATE OR REPLACE SECRET {secret.name} ({', '.join(clauses)})"
        con.execute(sql)

    def _resolve_catalog_backend(self) -> DuckLakeCatalogBackend | None:
        """Resolve the catalog backend to attach, if any."""
        if self.ducklake_catalog_backend is not None:
            return self.ducklake_catalog_backend
        if self.ducklake_catalog is not None:
            return PostgresDuckLakeCatalogBackend(
                self.ducklake_catalog,
                self.ducklake_extension,
            )
        return None

    def _resolve_s3_secret(self) -> S3SecretConfig | None:
        """Resolve the S3 secret independently from catalog attachment."""
        if self.s3_secret is not None:
            return self.s3_secret
        if self.ducklake_catalog is not None and self.ducklake_catalog.s3_secret is not None:
            return self.ducklake_catalog.s3_secret
        if all([self.bucket, self.access_key, self.secret_key, self.region]):
            return S3SecretConfig(
                key_id=self.access_key or "",
                secret=self.secret_key or "",
                region=self.region or "",
            )
        return None

    def _load_ducklake_support(
        self,
        con: duckdb.DuckDBPyConnection,
        backend: DuckLakeCatalogBackend | None,
    ) -> None:
        """Load DuckLake support when requested by config or backend."""
        if not (self.enable_ducklake or backend is not None or self.ducklake_extension is not None):
            return

        extension_names: list[str] = []
        if self.ducklake_extension is not None or self.enable_ducklake:
            extension = self.ducklake_extension or DuckLakeConfig()
            extension_names.extend(
                (
                    extension.extension_name,
                    extension.postgres_extension_name,
                )
            )
        if backend is not None:
            extension_names.extend(backend.required_extensions())

        self._load_extensions(con, extension_names)

    def _attach_catalog_backend(
        self,
        con: duckdb.DuckDBPyConnection,
        backend: DuckLakeCatalogBackend,
    ) -> None:
        """Attach a catalog backend to the connection."""
        backend.attach(con)

    def create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection configured for the requested storage mode."""
        con = duckdb.connect(database=self.database)
        self._load_httpfs(con)
        backend = self._resolve_catalog_backend()
        self._load_ducklake_support(con, backend)

        secret = self._resolve_s3_secret()
        if secret is not None:
            self._create_s3_secret(con, secret)

        if backend is not None:
            self._attach_catalog_backend(con, backend)
        return con


def create_duckdb_connection(
    *,
    bucket: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: Optional[str] = None,
    database: str = ":memory:",
    ducklake_extension: DuckLakeConfig | None = None,
    ducklake_catalog: DuckLakeCatalogConfig | None = None,
    ducklake_catalog_backend: DuckLakeCatalogBackend | None = None,
    s3_secret: S3SecretConfig | None = None,
    enable_ducklake: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Create a preconfigured DuckDB connection."""
    factory = DuckDBConnectionFactory(
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        database=database,
        ducklake_extension=ducklake_extension,
        ducklake_catalog=ducklake_catalog,
        ducklake_catalog_backend=ducklake_catalog_backend,
        s3_secret=s3_secret,
        enable_ducklake=enable_ducklake,
    )
    return factory.create_connection()


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
        ducklake_extension: Optional[DuckLakeConfig] = None,
        ducklake_catalog: Optional[DuckLakeCatalogConfig] = None,
        ducklake_catalog_backend: DuckLakeCatalogBackend | None = None,
        s3_secret: S3SecretConfig | None = None,
        enable_ducklake: bool = False,
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

        # Windows-specific DuckDB plugin handling
        if "win" in sys.platform:
            dir_name = os.path.dirname(os.path.abspath(__file__))
            plugin_files = glob.glob(os.path.join(dir_name, "plugins", "win64", "*.gz"))

            for plugin_path in plugin_files:
                try:
                    version = ".".join(plugin_path.split(".")[-3].split("_")[3:])
                except IndexError:
                    logger.error(f"Failed to parse plugin version: {plugin_path}")
                    continue

                if version == duckdb.__version__:
                    plugin_path_obj = Path(plugin_path)
                    install_plugin(
                        plugin_path_obj.name,
                        f"{plugin_path_obj.stem.split('_')[0]}.duckdb_extension",
                    )
                    logger.info(f"Installed plugin: {plugin_path_obj.name}")
                else:
                    logger.warning(
                        f"DuckDB version mismatch. "
                        f"DuckDB={duckdb.__version__}, Plugin={version}"
                    )

        self._con = create_duckdb_connection(
            bucket=self.bucket,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            ducklake_extension=ducklake_extension,
            ducklake_catalog=ducklake_catalog,
            ducklake_catalog_backend=ducklake_catalog_backend,
            s3_secret=s3_secret,
            enable_ducklake=enable_ducklake,
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

        last_modified_time = result[0]
        current_time = datetime.now()

        time_diff_seconds = (
            current_time - datetime.fromtimestamp(last_modified_time)
        ).total_seconds()

        if time_diff_seconds > lookback_delta_seconds:
            readable_time = datetime.fromtimestamp(last_modified_time).strftime("%Y-%m-%d %H:%M:%S")

            warnings.warn(f"File last modified at: {readable_time}")
            return True

        return False


def duckdb_datacacher(
    app_config: Optional[Dict[str, Any]] = None,
    bucket: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: Optional[str] = None,
    env_name: Optional[str] = None,
    ducklake_extension: Optional[DuckLakeConfig] = None,
    ducklake_catalog: Optional[DuckLakeCatalogConfig] = None,
    ducklake_catalog_backend: DuckLakeCatalogBackend | None = None,
    s3_secret: S3SecretConfig | None = None,
    enable_ducklake: bool = False,
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
        ducklake_extension=ducklake_extension,
        ducklake_catalog=ducklake_catalog,
        ducklake_catalog_backend=ducklake_catalog_backend,
        s3_secret=s3_secret,
        enable_ducklake=enable_ducklake,
        env_name=env_name,
    )
