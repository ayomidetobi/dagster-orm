"""Shared SQL-rendering primitives used by both duckdb_datacacher and ducklake_datacacher.

SQL/render_ducklake_sql/quote_identifier/sql_literal are the core building
blocks the rest of the codebase's query builders compose SQL with; join_s3/
collect_dataframes/install_plugin are small S3/DataFrame/Windows-plugin
helpers used by the legacy DuckDBDataCacher. Kept in one shared module so
neither duckdb_datacacher.py nor ducklake_datacacher.py has to duplicate them.
"""

from __future__ import annotations

import glob
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Any, Dict, Iterable

import duckdb
import pandas as pd
import structlog

from .config import S3SecretConfig

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

    Used by DuckDBDataCacher, whose _sql_to_string() substitutes each raw
    DataFrame binding for its relation name itself while resolving -- this
    just pre-registers them under the same f"df_{id(value)}" naming scheme.
    For render_ducklake_sql() (connection-free, no native DataFrame support),
    use extract_dataframe_bindings() instead, which also rewrites the SQL.

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


def extract_dataframe_bindings(sql_obj: SQL) -> tuple[SQL, Dict[str, pd.DataFrame]]:
    """Rewrite DataFrame bindings to relation-name identifiers, for rendering.

    render_ducklake_sql() is pure and connection-free, so it can't register a
    DataFrame binding against a connection the way DuckDBDataCacher's
    connection-bound _sql_to_string() does. This does both steps DuckLake
    callers need in one pass: returns a copy of sql_obj with every DataFrame
    binding replaced by an identifier (same f"df_{id(value)}" naming scheme
    as collect_dataframes(), so callers register each returned DataFrame
    under its paired relation name), plus the DataFrames to register.

    Args:
        sql_obj: SQL object with bindings

    Returns:
        (rewritten_sql, {relation_name: DataFrame}) -- register each
        DataFrame under its relation name before rendering rewritten_sql.
    """
    dataframes: Dict[str, pd.DataFrame] = {}
    new_bindings: Dict[str, Any] = {}

    for key, value in sql_obj.bindings.items():
        if isinstance(value, pd.DataFrame):
            relation = f"df_{id(value)}"
            dataframes[relation] = value
            new_bindings[key] = SQLIdentifier(relation)
        elif isinstance(value, SQL):
            nested_sql, nested_dfs = extract_dataframe_bindings(value)
            new_bindings[key] = nested_sql
            dataframes.update(nested_dfs)
        else:
            new_bindings[key] = value

    return SQL(sql_obj.sql, **new_bindings), dataframes


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


def load_extension(con: duckdb.DuckDBPyConnection, extension_name: str) -> None:
    """Install and load a single DuckDB extension.

    Shared by DuckDBConnectionFactory (ducklake_datacacher.py) and
    DuckDBDataCacher (duckdb_datacacher.py) -- neither is DuckLake-specific,
    so this lives here rather than being duplicated in (or owned by) either.
    """
    con.execute(f"INSTALL {extension_name}")
    con.execute(f"LOAD {extension_name}")


def load_extensions(con: duckdb.DuckDBPyConnection, extension_names: Iterable[str]) -> None:
    """Install and load extensions once, preserving order."""
    seen: set[str] = set()
    for extension_name in extension_names:
        if extension_name in seen:
            continue
        seen.add(extension_name)
        load_extension(con, extension_name)


def create_s3_secret(con: duckdb.DuckDBPyConnection, secret: S3SecretConfig) -> None:
    """Create or replace the DuckDB S3 secret used by httpfs.

    Shared by DuckDBConnectionFactory and DuckDBDataCacher -- both need an
    S3 secret for httpfs access, neither more so than the other.
    """
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
    con.execute(f"CREATE OR REPLACE SECRET {secret.name} ({', '.join(clauses)})")


def install_windows_plugins() -> None:
    """Install any matching Windows DuckDB plugins bundled with this package.

    No-op outside Windows. Looks for plugins under resources/plugins/win64/
    -- one level up from this duckdb_cacher/ package, matching the original
    (pre-restructure) lookup location.
    """
    if "win" not in sys.platform:
        return

    package_dir = os.path.dirname(os.path.abspath(__file__))
    resources_dir = os.path.dirname(package_dir)
    plugin_files = glob.glob(os.path.join(resources_dir, "plugins", "win64", "*.gz"))

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
                f"DuckDB version mismatch. DuckDB={duckdb.__version__}, Plugin={version}"
            )


def is_stale(observed_time: datetime, lookback_delta_seconds: int, *, label: str = "Last observed") -> bool:
    """Whether observed_time is older than lookback_delta_seconds ago.

    Shared by DuckDBDataCacher.staleness_check() (a Parquet file's mtime via
    pragma_storage_info) and DuckLakeDataCacher.staleness_check() (a DuckLake
    catalog's latest snapshot commit time) -- same comparison, different
    source of the timestamp.
    """
    now = datetime.now(observed_time.tzinfo) if observed_time.tzinfo else datetime.now()
    age_seconds = (now - observed_time).total_seconds()

    if age_seconds > lookback_delta_seconds:
        warnings.warn(f"{label}: {observed_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return True

    return False
