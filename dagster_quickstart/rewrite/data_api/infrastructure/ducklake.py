"""DuckLake infrastructure primitives."""

from __future__ import annotations

from typing import Any

import structlog

from resources.duckdb_datacacher import sql_literal
from rewrite.data_api.errors import DuckLakeConfigError
from rewrite.data_api.models.config import DuckLakeCatalogConfig, DuckLakeConfig

logger = structlog.get_logger(__name__)


class DuckLakeBootstrap:
    """Install and configure DuckLake on a DuckDB connection.

    DuckLake is treated as a DuckDB extension. The extension is installed and
    loaded first; PostgreSQL catalog and S3 storage settings are applied after
    that step.
    """

    def __init__(
        self,
        connection: Any,
        extension_config: DuckLakeConfig | None = None,
        catalog_config: DuckLakeCatalogConfig | None = None,
    ):
        """Initialize the bootstrapper."""
        self._connection = connection
        self._extension_config = extension_config or DuckLakeConfig()
        self._catalog_config = catalog_config or DuckLakeCatalogConfig()

    def install_ducklake_prerequisites(self) -> None:
        """Install DuckLake and the PostgreSQL extension."""
        logger.info(
            "ducklake_install_prerequisites",
            extension=self._extension_config.extension_name,
        )
        self._connection.execute(f"INSTALL {self._extension_config.extension_name}")
        self._connection.execute(f"INSTALL {self._extension_config.postgres_extension_name}")
        self._connection.execute(f"INSTALL {self._extension_config.httpfs_extension_name}")
        self._connection.execute(f"LOAD {self._extension_config.extension_name}")
        self._connection.execute(f"LOAD {self._extension_config.postgres_extension_name}")
        self._connection.execute(f"LOAD {self._extension_config.httpfs_extension_name}")

    def _postgres_attach_target(self) -> str:
        """Build the DuckLake PostgreSQL attach target."""
        if self._catalog_config.postgres is None:
            logger.warning("ducklake_missing_postgres_config")
            raise DuckLakeConfigError("postgres configuration is required for DuckLake attach")
        pg = self._catalog_config.postgres
        target = (
            "ducklake:postgres:"
            f"dbname={pg.database} host={pg.host} port={pg.port} "
            f"user={pg.user} password={pg.password}"
        )
        if pg.sslmode:
            target += f" sslmode={pg.sslmode}"
        return target

    def _attach_options_sql(self) -> str:
        """Build the DuckLake ATTACH options clause."""
        options: list[str] = [
            f"DATA_PATH '{self._catalog_config.data_path}'",
            f"METADATA_SCHEMA '{self._catalog_config.schema_name}'",
        ]
        if self._catalog_config.attach_options:
            options.extend(self._catalog_config.attach_options)
        return ", ".join(options)

    def attach_catalog(self) -> None:
        """Attach the DuckLake catalog using the PostgreSQL backend."""
        target = self._postgres_attach_target()
        options_sql = self._attach_options_sql()
        logger.info("ducklake_attach_catalog", catalog_alias=self._catalog_config.catalog_alias)
        sql = f"ATTACH '{target}' AS {self._catalog_config.catalog_alias} " f"({options_sql})"
        self._connection.execute(sql)

    def create_s3_secret(self) -> None:
        """Create or replace the DuckDB S3 secret used by httpfs."""
        if self._catalog_config.s3_secret is None:
            return
        secret = self._catalog_config.s3_secret
        logger.info("ducklake_create_s3_secret", secret_name=secret.name)
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
        sql = f"CREATE OR REPLACE SECRET {secret.name} " f"({', '.join(clauses)})"
        self._connection.execute(sql)

    def bootstrap(self) -> None:
        """Install the extensions and then attach the catalog."""
        logger.info("ducklake_bootstrap_started")
        self.install_ducklake_prerequisites()
        self.create_s3_secret()
        self.attach_catalog()
        logger.info("ducklake_bootstrap_completed")
