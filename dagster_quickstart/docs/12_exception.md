# MQL Datalake API

> **Document:** Error Handling
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

This document defines the error handling strategy for the MQL Datalake API.

Consistent error handling makes the platform easier to debug, test, maintain, and extend while ensuring implementation details remain hidden from consumers.

---

# Design Principles

Error handling should follow these principles:

- Fail early.
- Fail with meaningful messages.
- Never expose infrastructure details.
- Raise domain exceptions.
- Preserve the original exception for debugging.
- Log unexpected failures.
- Validate before execution whenever possible.

---

# Exception Hierarchy

Every custom exception should inherit from a common base exception.

```text
MQLException

├── ValidationError
│   ├── MetadataValidationError
│   ├── SchemaValidationError
│   └── ImportValidationError
│
├── QueryError
│   ├── InvalidQueryError
│   ├── SeriesNotFoundError
│   └── UnsupportedOperationError
│
├── RepositoryError
│   ├── ReadError
│   ├── WriteError
│   ├── TransactionError
│   └── DuplicateRecordError
│
├── DataSourceError
│   ├── ConnectorError
│   ├── InvalidResponseError
│   └── SourceUnavailableError
│
├── ImportError
│
├── ExportError
│
├── ConfigurationError
│
└── InternalError
```

Every exception should clearly communicate the problem at the domain level.

---

# Layer Responsibilities

Each layer is responsible for translating exceptions.

```
DataAPI

↓

Services

↓

Repositories

↓

DuckLake / SQLModel / Polars
```

Infrastructure exceptions should never escape the repository layer.

---

# Repository Layer

Repositories should translate infrastructure exceptions.

Example

```python
try:
    ...
except Exception as exc:
    raise RepositoryError(...) from exc
```

Repositories should never expose:

- DuckDB exceptions
- SQLAlchemy exceptions
- DuckLake exceptions
- Polars exceptions

---

# Service Layer

Services should raise business exceptions.

Examples:

- duplicate metadata
- invalid business rules
- unsupported operations

Services should not expose repository exceptions directly.

---

# Query Layer

QuerySet should validate user requests before execution.

Examples include:

- invalid filters
- unknown fields
- unsupported operations
- invalid sort columns

Errors should be descriptive and actionable.

---

# Validation Errors

Validation failures are expected behaviour.

They should:

- identify the invalid field
- explain why validation failed
- suggest corrective action when possible

Validation errors should not be logged as application errors.

---

# Import Errors

Import operations should stop immediately when validation fails.

The platform should never partially import invalid metadata.

Imports should remain transactional.

---

# Data Source Errors

Connectors should translate source-specific exceptions into platform exceptions.

Examples include:

- unavailable data source
- malformed response
- unsupported request

Business logic should never depend on connector-specific exception types.

---

# Logging

Unexpected exceptions should always be logged.

Expected validation failures generally should not.

Use structured logging with:

- operation
- request_id
- exception_type
- context

---

# Exception Chaining

Always preserve the original exception.

Example

```python
raise RepositoryError(...) from exc
```

This preserves debugging information while exposing only domain exceptions.

---

# Retry Strategy

The platform itself should not retry connector operations unless explicitly required.

Retry behaviour provided by external libraries or SDKs should not be duplicated.

---

# User Messages

Error messages should:

- explain what failed
- avoid implementation details
- provide useful context

Good

```
Series code "US10Y" does not exist.
```

Avoid

```
SQL Error 23505
```

---

# Testing

Every custom exception should have dedicated tests.

Verify:

- correct exception type
- meaningful message
- exception chaining
- logging behaviour (where appropriate)

---

# Anti-Patterns

Avoid:

- catching `Exception` without re-raising
- swallowing exceptions
- exposing infrastructure errors
- generic error messages
- using exceptions for normal control flow

---

# Summary

The MQL Datalake API exposes domain-focused exceptions while hiding infrastructure details.

Each layer translates lower-level failures into meaningful, platform-specific errors, ensuring a consistent developer experience and simplifying debugging.

---

# Next Reading

- 11_project_conventions.md