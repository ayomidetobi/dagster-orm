# MQL Datalake API

> **Document:** Architecture
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Software Engineers, Platform Engineers, Data Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the architecture of the MQL Datalake API.

It describes the responsibilities of every major component, how data flows through the system, and the architectural principles that guide implementation decisions.

This document is considered the authoritative reference for the internal architecture of the platform.

---

# Architectural Goals

The architecture is designed to satisfy the following goals:

- Maintain a stable public API.
- Hide vendor-specific implementations.
- Hide storage implementation details.
- Separate business logic from persistence.
- Support multiple market data vendors.
- Support scalable time-series storage.
- Maximize maintainability.
- Encourage testability.
- Enable future extensibility.

---

# High-Level Architecture

```
                     User

                       │

                 DataAPI (Public API)

                       │

                    QuerySet

                       │

                  Service Layer

         ┌─────────────┴─────────────┐

         │                           │

 MetadataService             ValueService

         │                           │

         ▼                           ▼

 MetadataRepository        ValueRepository

         │                           │

         ▼                           ▼

 PostgreSQL              DuckLakeRepository

                                      │

                                      ▼

                                  DuckLake

                                      │

                                      ▼

                                Object Storage
```

---

# Layered Architecture

The platform follows a layered architecture.

Each layer has a single responsibility.

Higher layers depend on lower layers.

Lower layers never depend on higher layers.

```
Client

↓

DataAPI

↓

QuerySet

↓

Services

↓

Repositories

↓

Infrastructure
```

Communication between layers is strictly one-directional.

---

# Architecture Principles

## Public API First

The public interface is the primary contract with consumers.

Internal implementations may change without affecting users.

The following interfaces are considered stable:

```python
api.get(...)
api.get_values(...)
QuerySet.value()
QuerySet.info()
QuerySet.last()
```

---

## Separation of Concerns

Each layer owns a single responsibility.

Business logic never exists inside repositories.

Storage logic never exists inside QuerySet.

Formatting never exists inside repositories.

Validation occurs only at system boundaries.

---

## Vendor Independence

The platform is independent of individual market data vendors.

Vendor implementations are interchangeable.

Supported vendors may include:

- Bloomberg
- MDS
- Hawk

Additional vendors should be introduced without modifying the public API.

---

## Storage Independence

Business logic must never depend on storage technology.

Consumers should not know whether data resides in:

- DuckLake
- PostgreSQL
- Amazon S3

Storage engines are implementation details.

---

# Component Responsibilities

## DataAPI

Responsibilities:

- Public entry point.
- Construct QuerySets.
- Expose semantic API.

Must never:

- Execute SQL.
- Read storage.
- Perform business logic.

---

## QuerySet

Responsibilities:

- Normalize filters.
- Build semantic queries.
- Coordinate services.
- Format returned data.
- Cache resolved metadata where appropriate.

Must never:

- Query PostgreSQL directly.
- Query DuckLake directly.
- Perform storage operations.

---

## Service Layer

The service layer contains business logic.

Examples:

- MetadataService
- ValueService
- VendorService
- ImportService

Responsibilities:

- Orchestrate repositories.
- Apply business rules.
- Handle validation.
- Coordinate transactions.

---

## Repository Layer

Repositories abstract persistence.

Responsibilities:

- Read data.
- Write data.
- Execute queries.

Repositories must never:

- Pivot data.
- Format responses.
- Apply business rules.
- Call other repositories.

---

## Infrastructure Layer

Infrastructure contains implementation-specific components.

Examples:

- DuckLake
- PostgreSQL
- Vendor clients
- Logging
- Configuration

Infrastructure should be replaceable without affecting business logic.

---

# Dependency Injection

All long-lived objects are managed using dependency-injector.

Example:

```
Container

↓

Settings

↓

Logger

↓

Database

↓

Repositories

↓

Services

↓

DataAPI
```

Business code never constructs dependencies directly.

---

# Validation Strategy

Validation occurs at system boundaries.

## Pydantic

Used for:

- Settings
- Requests
- Domain models
- Configuration

---

## Pandera

Used for:

- Vendor responses
- Metadata DataFrames
- DuckLake reads
- DuckLake writes

Every DataFrame crossing a repository boundary should be validated.

---

# Storage Architecture

The platform separates metadata from value storage.

## PostgreSQL

Stores:

- Metadata
- Vendor mappings
- Lookup tables
- Audit records
- Import history

---

## DuckLake

Stores:

- Time-series values

DuckLake manages:

- Snapshots
- Partitions
- Transactions
- File layout
- Object storage

The application never manages these directly.

---

# Metadata Flow

```
CSV / Excel

↓

Import Service

↓

Validation

↓

Transformation

↓

Metadata Repository

↓

PostgreSQL
```

---

# Value Retrieval Flow

```
DataAPI

↓

QuerySet

↓

ValueService

↓

ValueRepository

↓

DuckLakeRepository

↓

DuckLake

↓

Polars DataFrame

↓

Pandera Validation

↓

QuerySet

↓

User
```

---

# Vendor Ingestion Flow

```
Vendor

↓

Vendor Adapter

↓

Normalize

↓

Pandera Validation

↓

DuckLake Repository

↓

DuckLake
```

---

# Error Handling

Infrastructure exceptions should never propagate directly to users.

Repositories convert infrastructure errors into repository-level exceptions.

Services convert repository exceptions into domain exceptions.

The public API exposes only meaningful, actionable exceptions.

---

# Logging

The platform uses structlog.

Every request should include contextual information such as:

- request_id
- duration
- repository
- vendor
- row_count

Application code should never use print().

---

# Configuration

Configuration is managed through a single typed Settings object.

No component should access environment variables directly.

---

# Extensibility

The architecture should allow new implementations without changing existing business logic.

Examples:

- New vendor
- New storage engine
- New metadata field
- New query capability

Extension should occur by implementing interfaces rather than modifying existing components.

---

# Future Considerations

The architecture is designed to support future enhancements, including:

- Additional market data vendors
- Asynchronous execution
- Distributed query execution
- Multiple storage backends
- Additional analytical capabilities
- Cloud-native deployment

These enhancements should not require changes to the public API.

---

mql/

├── api/           # Public fluent API
├── query/         # QuerySet implementation
├── services/      # Business logic
├── repositories/  # Persistence
├── metadata/      # Metadata domain
├── vendors/       # External data source connectors
├── validation/    # Pydantic and Pandera schemas
├── models/        # SQLModel models
├── config/        # Settings and dependency injection
├── logging/       # Logging configuration
└── utils/         # Shared utilities only
# Related Documentation

- 01_introduction.md
- 03_engineering_standards.md
- 04_development_guide.md