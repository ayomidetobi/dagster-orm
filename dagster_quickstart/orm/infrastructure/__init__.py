"""Infrastructure layer for DuckDB ORM.

Contains low-level adapters and repository implementations.
"""

from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.infrastructure.temp_table_manager import TempTableManager

__all__ = [
    "DuckDbRepository",
    "ParquetAdapter",
    "S3Adapter",
    "TempTableManager",
]
