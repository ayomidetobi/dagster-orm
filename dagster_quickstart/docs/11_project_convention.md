# MQL Datalake API

> **Document:** Project Conventions
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Software Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the project-specific conventions used throughout the MQL Datalake API.

Unlike the Engineering Standards, which describe general software engineering practices, this document captures conventions unique to the MQL Datalake API.

These conventions ensure that all contributors implement features consistently and preserve the architecture as the project evolves.

---

# General Principles

Every implementation should:

- Follow the layered architecture.
- Be easy to extend.
- Hide implementation details.
- Prefer composition over inheritance.
- Be fully typed.
- Be documented.
- Be independently testable.

---

# Public API

The public API is the primary contract between the library and its consumers.

Public interfaces should remain stable even if the internal implementation changes.

Examples

```python
api.get(...)

api.get_values(...)

api.get_last_value(...)

query.value()

query.info()
```

Consumers should never interact directly with:

- DuckLake
- SQLModel
- SQLAlchemy
- Polars internals
- Vendor adapters
- Repository classes

---

# QuerySet

QuerySet represents a semantic financial query.

Every QuerySet operation should be lazy until execution is requested.

Examples

```python
.value()

.info()

.last()

.to_polars()
```

Creating a QuerySet must never execute a database query.

---

# Services

Every service should end with **Service**.

Examples

```
MetadataService

ValueService

ImportService

VendorService
```

Services:

- own business logic
- orchestrate repositories
- coordinate validation
- remain storage agnostic

Services must never execute SQL directly.

---

# Repositories

Every repository should end with **Repository**.

Examples

```
DuckLakeRepository

MetadataRepository
```

Repositories:

- communicate with DuckLake
- execute SQL
- execute analytical queries
- map persistence objects

Repositories must never:

- implement business rules
- format responses
- perform orchestration

---

# SQLModel Models

Each SQLModel model represents a database table.

Models describe:

- structure
- constraints
- relationships

Models must never contain business logic.

---

# Pydantic Models

Use Pydantic for:

- configuration
- API requests
- API responses
- domain models

Do not use Pydantic models as database models.

---

# Pandera Schemas

Every DataFrame crossing a repository boundary should be validated.

Examples

Vendor response

↓

Pandera

↓

Business logic

DuckLake

↓

Pandera

↓

Service layer

Validation should occur as early as possible.

---

# DuckLake

DuckLake is the single persistence platform for the project.

DuckLake manages:

- PostgreSQL catalog
- table metadata
- snapshots
- transactions
- object storage

Application code should never manipulate:

- Parquet files
- snapshot metadata
- object storage layout
- partition structure

These concerns belong to DuckLake.

---

# Metadata

Metadata should always be represented using semantic financial concepts.

Examples

- Asset Class
- Product
- Currency
- Country
- Tenor
- Frequency
- Vendor

Business logic should never depend on vendor-specific identifiers.

---

# Vendor Adapters

Every vendor integration should implement a common interface.

Example

```
VendorAdapter

BloombergAdapter

MDSAdapter

FutureVendorAdapter
```

Adapters should:

- retrieve data
- normalize data
- validate responses

Adapters must never write directly to DuckLake.

---

# Import Pipeline

Metadata imports should always follow the same workflow.

```
CSV / Excel

↓

Validation

↓

Transformation

↓

Business Validation

↓

Repository

↓

DuckLake Catalog
```

Skipping validation stages is prohibited.

---

# Data Ingestion

All vendor data should be normalized before persistence.

Pipeline

```
Vendor

↓

Adapter

↓

Pandera Validation

↓

Normalization

↓

Repository

↓

DuckLake
```

Business logic should never consume raw vendor responses.

---

# Exceptions

Every custom exception should inherit from a common project exception.

Example

```
MQLException

├── ValidationError

├── RepositoryError

├── QueryError

├── VendorError

└── ImportError
```

Never expose infrastructure exceptions directly to users.

---

# Logging

All logging should use **structlog**.

Every significant operation should include contextual information.

Examples

- request_id
- query_duration
- row_count
- vendor
- series_count
- repository

Avoid logging unnecessary or sensitive data.

---

# Configuration

All configuration should come from the typed Settings object.

Never read environment variables outside the configuration layer.

---

# Constants

Store constants close to the feature that owns them.

Example

```
core/constants.py

metadata/constants.py

vendors/constants.py

query/constants.py
```

Avoid a single monolithic constants module.

---

# Enums

Prefer `StrEnum` over plain strings.

Examples

```
Vendor

AssetClass

Frequency

Country

Currency
```

Enums should represent finite domain values.

---

# Utilities

Utility modules should contain only reusable helper functions.

Utilities must never contain business logic.

If a helper depends on repositories or services, it belongs in a service instead.

---

# Domain Models

Domain models should represent business concepts rather than storage structures.

Examples

```
Series

Metadata

VendorMapping

TimeSeries
```

Avoid exposing SQLModel objects outside the repository layer.

---

# Polars

Polars is the internal DataFrame library.

Repositories and services should exchange Polars DataFrames where appropriate.

Avoid unnecessary conversions between Polars and Pandas.

---

# Dependency Injection

Every long-lived dependency should be created by the dependency injection container.

Examples

- logger
- settings
- repositories
- services
- caches

Application code should never instantiate these directly.

---

# Caching

Only cache data that changes infrequently.

Suitable examples

- metadata
- lookup tables
- vendor mappings

Avoid caching time-series values unless there is a demonstrated need.

---

# Method Design

Public methods should:

- have a single responsibility
- be fully typed
- include Google Style docstrings
- return predictable types
- avoid hidden side effects

Methods should not mutate inputs unless explicitly documented.

---

# Adding New Metadata Fields

When introducing a new metadata field, update:

- SQLModel model
- Alembic migration
- Pandera schema
- Pydantic model
- import pipeline
- export pipeline
- validation
- documentation
- tests

---

# Adding a New Vendor

When introducing a new vendor, implement:

- Vendor Adapter
- Validation schema
- Normalization logic
- Import pipeline
- Integration tests
- Documentation

The public API should remain unchanged.

---

# Adding a New Public API Method

Every new public method should include:

- Complete type hints
- Google Style docstring
- Usage example
- Unit tests
- Integration tests
- Documentation updates

The method should follow existing naming conventions and preserve the semantic design of the API.

---

# Architecture Rules

Never bypass the architecture.

Allowed

```
DataAPI

↓

QuerySet

↓

Service

↓

Repository

↓

DuckLake
```

Not Allowed

```
DataAPI

↓

DuckLake
```

or

```
Service

↓

S3
```

Every component should communicate only with the layer immediately below it.

---

# Backward Compatibility

The public API is considered stable.

Internal implementations may change freely provided that the public interface remains consistent.

---

# Documentation

Every public class, method, and module should be documented.

Architecture changes should always be reflected in the documentation.

Documentation is considered part of the implementation and should be updated alongside code changes.

---

# Summary

The conventions in this document define how the MQL Datalake API is implemented beyond general engineering practices.

Following these conventions ensures the platform remains:

- Consistent
- Predictable
- Extensible
- Maintainable

Every new feature should integrate naturally into the existing architecture without introducing new patterns unnecessarily.
