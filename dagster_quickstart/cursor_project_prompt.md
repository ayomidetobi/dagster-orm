# Cursor System Prompt – Data API Rewrite

## Objective
Redesign the project with a greenfield rewrite.

Do not preserve backward compatibility shims, deprecated aliases, or legacy implementation details.
Remove code freely when it simplifies the architecture.

## Architecture

Client
→ DataAPI
→ QuerySet
→ Services
→ Repositories
→ DuckLakeRepository
→ DuckLake (catalog onpostgres time-series on S3)

Metadata is stored in Ducklake postgres catalog.

### Technologies
- DuckLake
- SQLModel
- Alembic
- Pydantic v2
- Pandera
- Polars (preferred internally)
- PyArrow
- structlog
- dependency-injector
- cachetools
- Dagster

### Responsibilities

DataAPI
- Public API only.
- No SQL or storage logic.

QuerySet
- Filter normalization
- Orchestration
- Output formatting
- Never accesses storage directly.

Services
- Business logic only.

Repositories
- Persistence only.

DuckLakeRepository
- Only component aware of DuckLake.

MetadataRepository
- SQLModel + PostgreSQL. (if needed)

### Validation

Use:
- Pydantic for configuration, requests and domain models.
- Pandera for DataFrame validation before reads/writes.

### Logging

Use structlog.
Bind request IDs and context.
JSON logs in production.

### Dependency Injection

Wire services using dependency-injector.
Avoid manual object construction.

### Database

Use SQLModel models.(if needed)
Manage schema exclusively with Alembic migrations.(if needed)

### Metadata Import

CSV/Excel
→ Pandera validation
→ staging table
→ validation
→ merge/upsert
→ production metadata.

### Testing

Use pytest.
Mock repositories via dependency injection.

Never bypass service boundaries.
