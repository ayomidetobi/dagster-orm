# MQL Datalake API

> **Document:** Development Guide
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

This guide explains how to develop, extend, and maintain the MQL Datalake API.

Unlike the Engineering Standards, which define *how code should be written*, this guide explains *how new features should be added* while preserving the platform architecture.

Every new feature should integrate naturally into the existing architecture rather than introducing new patterns.

---

# Development Workflow

Every feature should follow the same development lifecycle.

```
Understand Requirement

↓

Design

↓

Implement

↓

Validate

↓

Test

↓

Document

↓

Review

↓

Merge
```

Do not skip any stage.

---

# Before Writing Code

Before implementing a feature, ask the following questions:

- Does similar functionality already exist?
- Which layer owns this responsibility?
- Will this change the public API?
- Can an existing component be extended?
- Does this require database changes?
- Does documentation need updating?

If the answer is unclear, revisit the architecture before writing code.

---

# Layer Responsibilities

Always determine where new functionality belongs.

| Layer | Responsibility |
|---------|---------------|
| DataAPI | Public interface |
| QuerySet | Query construction |
| Service | Business logic |
| Repository | Persistence |
| Infrastructure | External systems |

Business logic should never be implemented in repositories.

Storage logic should never be implemented in QuerySet.

---

# Feature Development Checklist

Every feature should include:

- Business logic
- Validation
- Logging
- Tests
- Documentation
- Type hints
- Google Style docstrings

---

# Adding a New Service

Create a new service when introducing new business capabilities.

Example

```
services/

    import_service.py

    analytics_service.py

    cache_service.py
```

Services should:

- coordinate repositories
- contain business rules
- remain storage agnostic

Services must never access DuckLake directly.

---

# Adding a New Repository

Repositories should only be created when introducing new persistence responsibilities.

Repositories:

- read
- write
- execute queries

Repositories must not contain business logic.

---

# Adding a New Metadata Field

When adding metadata:

Update:

- SQLModel model
- Alembic migration
- Pydantic model
- Pandera schema
- Import validation
- Export logic
- Repository
- Tests
- Documentation

Ensure backward compatibility unless intentionally introducing a breaking change.

---

# Adding a New Vendor

Every vendor follows the same architecture.

```
Vendor

↓

Vendor Adapter

↓

Normalization

↓

Pandera Validation

↓

Repository

↓

DuckLake
```

A vendor integration should include:

- Adapter
- Validation
- Normalization
- Integration tests
- Documentation

The public API should not change.

---

# Adding a New Public API Method

Every new API method should:

- follow existing naming conventions
- support fluent chaining where appropriate
- include complete type hints
- include Google Style docstrings
- include usage examples
- include tests
- update documentation

Avoid introducing multiple ways to accomplish the same task.

---

# Database Changes

Schema changes should always use Alembic.

Never modify production tables manually.

Every migration should be:

- reversible
- reviewed
- tested

Update SQLModel models and documentation together.

---

# Validation

Every new input should be validated.

Use:

- Pydantic for objects
- Pandera for DataFrames

Validation belongs at system boundaries.

---

# Logging

Every significant operation should log:

- operation
- duration
- affected rows
- repository
- request identifier

Use structured logging with `structlog`.

---

# Error Handling

Introduce custom exceptions where appropriate.

Do not expose:

- SQL exceptions
- DuckLake exceptions
- Infrastructure errors

Translate infrastructure failures into meaningful domain exceptions.

---

# Testing

Every feature requires:

## Unit Tests

Business logic

## Integration Tests

Repository interactions

## End-to-End Tests

Complete workflow validation

No feature is complete without tests.

---

# Documentation

Documentation is part of the feature.

When implementing new functionality, update:

- API documentation
- Architecture documentation (if necessary)
- Engineering standards (if conventions change)
- Usage examples

---

# Code Review

Every pull request should answer:

- Is the feature in the correct layer?
- Is the implementation consistent with existing architecture?
- Is validation complete?
- Are tests included?
- Are logs meaningful?
- Is documentation updated?
- Does the implementation avoid duplication?

---

# Performance Considerations

Before optimizing:

Measure performance.

Prefer:

- Polars expressions
- Predicate pushdown
- Projection pushdown
- Bulk operations

Avoid premature optimization.

---

# Common Mistakes

Avoid:

- Business logic inside repositories
- Direct DuckLake access from services
- Hardcoded configuration
- Duplicate validation
- Copy-paste code
- Missing tests
- Missing documentation

---

# Definition of Done

A feature is complete when:

- Code is implemented.
- Tests pass.
- Documentation is updated.
- Logging is included.
- Validation is complete.
- Type hints are complete.
- Google Style docstrings are written.
- Code review feedback is addressed.

If any of these are missing, the feature is not complete.

---

# Summary

Every contribution should strengthen the platform rather than increase complexity.

The preferred approach is to extend the existing architecture while preserving consistency, readability, and maintainability.

Development should always follow the architecture, engineering standards, and project conventions defined throughout this documentation.

---

# Next Reading

- 07_testing_strategy.md
- 08_logging_observability.md