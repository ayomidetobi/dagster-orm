"""DuckLake connection/attach machinery.

Everything here is specific to attaching a DuckLake catalog (PostgreSQL
metadata + S3 storage) to a DuckDB connection -- separate from
duckdb_datacacher.py's plain S3-Parquet cache, which has no notion of
DuckLake at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

import duckdb
import pandas as pd
import structlog

from .config import DuckLakeCatalogConfig, DuckLakeConfig, S3SecretConfig
from .exceptions import DuckLakeConfigError
from .utils import (
    SQL,
    create_s3_secret,
    extract_dataframe_bindings,
    is_stale,
    load_extension,
    load_extensions,
    render_ducklake_sql,
    sql_literal,
)

logger = structlog.get_logger(__name__)


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

    def _load_httpfs(self, con: duckdb.DuckDBPyConnection) -> None:
        """Install and load httpfs for S3 access."""
        extension = self.ducklake_extension or DuckLakeConfig()
        load_extension(con, extension.httpfs_extension_name)

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

        load_extensions(con, extension_names)

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
            create_s3_secret(con, secret)

        if backend is not None:
            self._attach_catalog_backend(con, backend)
        return con


class DuckLakeDataCacher:
    """DuckLake-backed cache: save()/load()/staleness_check() against DuckLake tables.

    Mirrors DuckDBDataCacher's shape (resources/duckdb_cacher/duckdb_datacacher.py),
    but targets a DuckLake-attached connection's catalog tables instead of raw
    S3 Parquet files. SQL objects are resolved via the shared, connection-free
    render_ducklake_sql() rather than DuckDBDataCacher's file_path/Parquet
    -encryption-aware resolution -- DuckLake tables have no file paths to
    resolve, so that machinery doesn't apply here.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, *, catalog_alias: str) -> None:
        self._con = con
        self._catalog_alias = catalog_alias

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Get the DuckDB connection."""
        return self._con

    def save(self, select_statement: SQL, table_name: str, *, debug: bool = False) -> bool:
        """Append a query result into a DuckLake-managed table.

        Args:
            select_statement: SQL object with query and bindings
            table_name: Destination DuckLake table (append-only insert)
            debug: If True, log the generated query

        Returns:
            True if the insert was successful

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        if select_statement is None:
            raise ValueError("select_statement is None")

        if not isinstance(select_statement, SQL):
            raise ValueError(f"Expected SQL; got {type(select_statement)}")

        resolved_select, dataframes = extract_dataframe_bindings(select_statement)
        for relation, frame in dataframes.items():
            self._con.register(relation, frame)

        query = render_ducklake_sql(
            SQL(
                "INSERT INTO $table $select",
                table=SQL.identifier(table_name),
                select=resolved_select,
            )
        )

        if debug:
            logger.info(f"QUERY: {query}")

        self._con.execute(query)
        return True

    def load(self, select_statement: SQL, *, debug: bool = False) -> Optional[pd.DataFrame]:
        """Load a query result from DuckLake into a Pandas DataFrame.

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

        resolved_select, dataframes = extract_dataframe_bindings(select_statement)
        for relation, frame in dataframes.items():
            self._con.register(relation, frame)

        query = render_ducklake_sql(resolved_select)

        if debug:
            logger.info(f"QUERY: {query}")

        result = self._con.execute(query)

        if result is None:
            return None

        df = result.df()
        return None if df.empty else df

    def staleness_check(self, lookback_delta_seconds: int) -> bool:
        """Check whether the DuckLake catalog's most recent snapshot is stale.

        Unlike DuckDBDataCacher.staleness_check() (a Parquet file's mtime via
        pragma_storage_info), DuckLake tracks commit history natively via
        ducklake_snapshots() -- there's no separate file to inspect.

        Args:
            lookback_delta_seconds: Maximum age in seconds before considered stale

        Returns:
            True if stale, False if fresh or unknown
        """
        try:
            result = self._con.execute(
                f"SELECT max(snapshot_time) FROM ducklake_snapshots({sql_literal(self._catalog_alias)})"
            ).fetchone()
        except Exception as exc:
            logger.error(f"Failed to fetch DuckLake snapshot info: {exc}")
            return False

        if not result or result[0] is None:
            logger.error("No snapshot data available.")
            return False

        return is_stale(result[0], lookback_delta_seconds, label="Catalog last committed at")


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
