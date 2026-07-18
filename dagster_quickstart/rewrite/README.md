# Rewrite Package

This package is a side-by-side scaffold for the DuckLake rewrite.

Goals:
- keep the public API shape stable
- separate API, service, repository, model, vendor, and ingestion concerns
- centralize DuckLake access behind one repository boundary
- treat DuckLake as a DuckDB extension that is installed and loaded before catalog and S3 settings are applied
- install `ducklake`, `postgres`, and `httpfs`, create an S3 secret with `CREATE OR REPLACE SECRET ... TYPE s3`, then `ATTACH 'ducklake:postgres:...' AS ... (DATA_PATH '...')`
- use structlog for structured logging throughout the rewrite package
- remove storage-specific knowledge from higher layers

Local setup:
- create a virtual environment in `.venv`
- install `structlog` into that environment
- install `dependency-injector` into that environment
- call `rewrite.configure_logging()` during application startup
- use `rewrite.create_container()` or `rewrite.create_data_api()` to build the rewrite stack

The package is intentionally isolated from the current `orm/` implementation so we can migrate incrementally.
