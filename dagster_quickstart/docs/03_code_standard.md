# MQL Datalake API

> **Document:** Engineering Standards
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Software Engineers, Data Engineers, Platform Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the engineering standards for the MQL Datalake API.

These standards exist to ensure the codebase remains:

- Readable
- Maintainable
- Testable
- Extensible
- Consistent
- Performant

Every contribution to this project should follow these standards.

---

# Engineering Philosophy

The MQL Datalake API is a long-lived platform intended to evolve over many years.

Engineering decisions should prioritize:

- Simplicity over cleverness
- Readability over brevity
- Maintainability over premature optimization
- Explicit code over implicit behavior
- Strong typing over dynamic typing
- Composition over inheritance
- Small focused components over large generic classes

Code should be understandable by someone reading it six months later without additional explanation.

---

# Core Principles

Every component should follow these principles.

- Single Responsibility
- Separation of Concerns
- Explicit Dependencies
- Strong Typing
- Testability
- Extensibility
- Reusability
- Performance by Design

---

# Project Structure

The project follows a layered architecture.

```
mql/

├── api/
├── core/
├── config/
├── services/
├── repositories/
├── infrastructure/
├── vendors/
├── metadata/
├── ingestion/
├── query/
├── models/
├── schemas/
├── validators/
├── utils/
└── tests/
```

Every package should have a clearly defined responsibility.

---

# Layer Responsibilities

## API Layer

Responsible for exposing the public API.

Must never:

- execute SQL
- access DuckLake
- contain business logic

---

## Query Layer

Responsible for semantic query construction and orchestration.

Must never communicate directly with storage.

---

## Service Layer

Responsible for:

- business rules
- orchestration
- validation
- transactions

Services may coordinate multiple repositories.

---

## Repository Layer

Responsible only for persistence.

Repositories:

- read data
- write data
- execute queries

Repositories must never contain business rules.

---

## Infrastructure Layer

Responsible for external systems.

Examples:

- DuckLake
- PostgreSQL
- S3
- Vendor clients
- Logging
- Configuration

---

# SOLID Principles

All code should naturally follow SOLID.

## Single Responsibility Principle

Each class should have one reason to change.

---

## Open / Closed Principle

Prefer extending components over modifying existing implementations.

---

## Liskov Substitution Principle

Implementations should be interchangeable.

---

## Interface Segregation Principle

Prefer focused interfaces.

Avoid large interfaces with unrelated responsibilities.

---

## Dependency Inversion Principle

Depend on abstractions rather than concrete implementations.

---

# Python Standards

The project targets **Python 3.12+**.

Prefer modern language features including:

- pathlib
- match
- Protocol
- Self
- StrEnum
- slots where appropriate
- dataclasses
- type aliases

---

# Naming Conventions

Good names reduce the need for comments.

## General Rules

1. Use descriptive, self-explanatory names.
2. Prefer readability over brevity.
3. Avoid cryptic abbreviations.
4. Name variables by intent rather than implementation detail.
5. Use consistent financial domain terminology.

Good

```python
metadata_repository
vendor_mapping
validated_dataframe
series_codes
```

Avoid

```python
tmp
df2
repo
obj
data1
x
```

---

## Boolean Variables

Boolean variables should read naturally.

Good

```python
is_active
has_permission
can_execute
should_refresh
is_metadata_loaded
```

Avoid

```python
active
permission
refresh
flag
```

---

## Function Names

Functions should describe actions.

Good

```python
load_metadata()

fetch_values()

validate_dataframe()

save_metadata()
```

---

## Class Names

Classes should represent nouns.

Examples

```python
MetadataRepository

ValueService

ImportService

DuckLakeRepository
```

---

# Type Hints

Every public function, method and class must include complete type hints.

Example

```python
def get_values(
    series_codes: list[str],
    start: datetime,
    end: datetime,
) -> pl.DataFrame:
```

Avoid using `Any`.

If `Any` is required, document why.

---

# Documentation Standards

Every public:

- module
- class
- function
- method

must include **Google Style docstrings**.

Example

```python
def value(self) -> pl.DataFrame:
    """Retrieve values for the current query.

    Returns:
        A Polars DataFrame containing the requested
        time-series values.

    Raises:
        QueryExecutionError:
            Raised when the query cannot be executed.
    """
```

Comments should explain **why**, not **what**.

Avoid obvious comments.

Bad

```python
# increment i
i += 1
```

---

# File and Function Organization

## Functions

Aim for functions under **50 lines**.

If a function exceeds approximately **75 lines**, consider extracting helper methods.

---

## Classes

Classes should have one responsibility.

If a class grows beyond **500 lines**, evaluate whether it should be split.

---

## Files

Files should generally remain under **500 lines**.

Large modules usually indicate multiple responsibilities.

---

# Code Organization

Group code in the following order.

1. Constants
2. Enums
3. Models
4. Public classes
5. Private helpers

Public methods should appear before private methods.

---

# Dependency Injection

The project uses **dependency-injector**.

Business code should never instantiate:

- repositories
- services
- databases
- caches
- loggers

Dependencies should always be injected.

---

# Repository Standards

Repositories are persistence adapters.

Repositories may:

- execute SQL
- execute DuckLake queries
- read
- write

Repositories must never:

- contain business logic
- format results
- pivot DataFrames
- call other repositories

---

# Service Standards

Services own business logic.

Services coordinate repositories.

Services are responsible for:

- orchestration
- validation
- transactions
- business rules

---

# QuerySet Standards

QuerySet represents semantic financial queries.

QuerySet should:

- normalize filters
- coordinate services
- format results

QuerySet must never communicate directly with storage.

---

# DataFrame Standards

The project uses **Polars** internally.

Pandas should only be used when required by the public API or external libraries.

Avoid unnecessary conversions.

Prefer vectorized expressions.

Avoid row-by-row loops.

---

# Validation Standards

## Pydantic

Use for:

- settings
- configuration
- requests
- responses
- domain models

---

## Pandera

Use for:

- metadata DataFrames
- vendor DataFrames
- value DataFrames

Every DataFrame entering the business layer should be validated.

---

# Database Standards

Metadata is stored in PostgreSQL using SQLModel.

Schema evolution is managed exclusively through Alembic.

Never modify production schemas manually.

DuckLake is the only storage engine for time-series values.

---

# Logging Standards

The project uses **structlog** exclusively.

Never use:

```python
print(...)
```

Logs should be structured.

Example

```python
logger.info(
    "metadata_loaded",
    row_count=1000,
    duration_ms=80,
)
```

Include meaningful contextual information.

---

# Error Handling

Never silently ignore exceptions.

Bad

```python
except Exception:
    pass
```

Catch specific exceptions whenever possible.

Raise meaningful domain exceptions.

Infrastructure exceptions should not leak into the public API.

---

# Configuration Standards

Configuration belongs in a typed `Settings` object.

Never scatter `os.getenv()` throughout the project.

Configuration should be loaded once during application startup.

---

# Constants & Enums

## Constants

Avoid hardcoding repeated values.

Store constants in dedicated `constants.py` modules.

Examples

```
core/constants.py

vendors/constants.py

metadata/constants.py
```

Avoid creating one large global constants file.

---

## Enums

Use `StrEnum` for categorical values whenever appropriate.

Examples

- Vendor
- AssetClass
- Frequency
- Currency
- Country

Enums improve readability and reduce typographical errors.

---

# Avoid Magic Values

Do not hardcode repeated strings or numbers.

Bad

```python
if vendor == "Bloomberg":
```

Good

```python
if vendor == Vendor.BLOOMBERG:
```

Bad

```python
timeout = 30
```

Good

```python
timeout = DEFAULT_TIMEOUT_SECONDS
```

Configuration values should never be hardcoded.

Examples include:

- bucket names
- retry counts
- cache durations
- feature flags

---

# DRY (Don't Repeat Yourself)

Business logic should exist in one place only.

Avoid copy-paste programming.

Extract duplicated logic into reusable:

- functions
- services
- repositories
- utilities
- validators

Validation rules should be reusable.

Polars transformations used in multiple places should be extracted into helper functions.

Before writing new code ask:

- Does this already exist?
- Can it be reused?
- Can it be generalized?

---

# Performance Guidelines

Optimise only after measuring.

Prefer:

- bulk operations
- vectorized DataFrame operations
- lazy execution
- push-down filtering
- caching metadata rather than values

Avoid unnecessary loops.

Avoid unnecessary DataFrame copies.

---

# Testing Standards

Every feature should include tests.

Testing pyramid:

- Unit Tests
- Integration Tests
- End-to-End Tests

Business logic should be tested independently of infrastructure.

Repositories should be tested against real infrastructure where practical.

---

# Code Review Checklist

Before merging code ask:

- Is it readable?
- Does it follow the architecture?
- Does each class have a single responsibility?
- Are type hints complete?
- Are Google Style docstrings present?
- Is validation performed?
- Are logs meaningful?
- Is duplicated code avoided?
- Are constants centralized?
- Are magic values avoided?
- Are tests included?

---

# Anti-Patterns

Avoid:

- God classes
- Circular dependencies
- Global state
- Hidden side effects
- Long methods
- Duplicate business logic
- Copy-paste programming
- Silent exception handling
- Wildcard imports
- Mutable default arguments
- Hardcoded configuration
- Direct repository construction
- Direct database access outside repositories

---

# Summary

The MQL Datalake API is designed to be a long-lived, maintainable platform.

Every engineering decision should prioritize:

- readability
- maintainability
- consistency
- extensibility
- testability

The public API should remain stable while internal implementations continue to evolve.

---

# Next Reading

- 04_database_design.md
- 05_api_design.md