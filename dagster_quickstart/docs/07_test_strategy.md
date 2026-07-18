# MQL Datalake API

> **Document:** Testing Strategy
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Software Engineers, Data Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the testing strategy for the MQL Datalake API.

Testing is a first-class engineering practice and is considered part of the implementation, not a separate phase of development.

The goal is to ensure every component is correct, maintainable, and resilient to future changes while providing confidence that financial data is accurate throughout the platform.

---

# Testing Philosophy

The MQL Datalake API follows these testing principles:

- Test behaviour rather than implementation.
- Every bug should result in a regression test.
- Tests should be deterministic.
- Tests should be isolated.
- Tests should be easy to read.
- Tests should document business behaviour.
- Prefer real infrastructure over mocks whenever practical.
- Financial correctness is more important than implementation details.

A good test should explain **what** the system is expected to do rather than **how** it does it.

---

# Testing Pyramid

The project follows the classic testing pyramid.

```
              End-to-End Tests

            Integration Tests

               Unit Tests
```

Most tests should be unit tests.

Integration tests verify interactions between components.

End-to-end tests validate complete user workflows.

---

# Test Directory Structure

The test suite mirrors the application architecture.

```text
tests/

├── unit/
│   ├── api/
│   ├── query/
│   ├── services/
│   ├── repositories/
│   ├── validators/
│   ├── models/
│   └── utils/
│
├── integration/
│   ├── ducklake/
│   ├── metadata/
│   ├── vendors/
│   ├── imports/
│   └── repositories/
│
├── e2e/
│
├── factories/
│   ├── metadata_factory.py
│   ├── timeseries_factory.py
│   ├── vendor_factory.py
│   ├── queryset_factory.py
│   └── request_factory.py
│
├── fixtures/
│
├── data/
│
└── conftest.py
```

The test structure should evolve alongside the application structure.

---

# Types of Tests

## Unit Tests

Unit tests verify a single component in isolation.

Characteristics:

- Fast
- Deterministic
- No network
- No external services
- No database dependencies unless unavoidable

Examples:

- Query building
- Business rules
- Validators
- Utility functions
- Configuration

---

## Integration Tests

Integration tests verify interactions between multiple components.

Examples include:

- SQLModel ↔ DuckLake Catalog
- Repository ↔ DuckLake
- Import Pipeline
- Vendor Adapters
- Metadata persistence
- Dependency Injection

Integration tests should use real infrastructure whenever practical.

---

## End-to-End Tests

End-to-end tests validate complete user workflows.

Example workflow:

```
CSV

↓

Metadata Import

↓

Validation

↓

DuckLake Catalog

↓

Query Metadata

↓

Retrieve Time-Series Values
```

End-to-end tests should exercise the platform exactly as users interact with it.

---

# Domain-Driven Test Factories

The project uses **custom test factories** rather than relying heavily on ORM factories.

Factories generate reusable domain-specific test data that reflects real financial concepts.

Factories improve:

- readability
- consistency
- maintainability
- reusability

Factories should represent business concepts rather than storage structures.

---

# Factory Design Principles

Factories should:

- produce valid objects by default
- support overriding individual fields
- hide construction complexity
- generate deterministic data
- model real financial data
- remain independent of business logic

Factories should never perform validation or persistence.

Their only responsibility is generating test data.

---

# MetadataFactory

Responsible for creating metadata records.

Examples

```python
metadata = MetadataFactory.create()

metadata = MetadataFactory.create(
    currency="USD",
    asset_class="Rates",
)
```

The generated metadata should always be valid unless explicitly creating invalid test data.

---

# TimeSeriesFactory

Responsible for generating realistic financial time-series.

Examples

```python
TimeSeriesFactory.create()

TimeSeriesFactory.create(rows=1000)

TimeSeriesFactory.usd_rates()

TimeSeriesFactory.eur_fx()

TimeSeriesFactory.swap_curve()
```

Generated data should resemble real market datasets whenever practical.

---

# VendorResponseFactory

Responsible for generating representative vendor responses.

Examples

```python
VendorResponseFactory.bloomberg()

VendorResponseFactory.mds()
```

Vendor responses should mimic real API responses while remaining deterministic.

No external vendor should be contacted during tests.

---

# QueryFactory

Responsible for generating semantic queries.

Examples

```python
QueryFactory.fx()

QueryFactory.rates()

QueryFactory.country("US")

QueryFactory.currency("EUR")
```

This avoids repeatedly constructing identical queries throughout the test suite.

---

# RequestFactory

Responsible for generating request models.

Examples

```python
RequestFactory.metadata_import()

RequestFactory.value_request()
```

Use request factories instead of manually creating request objects inside tests.

---

# Factory Guidelines

Factories should always return valid objects by default.

Good

```python
MetadataFactory.create(currency="USD")
```

Good

```python
MetadataFactory.invalid_currency()
```

Avoid manually constructing large objects inside individual tests.

Factories should encapsulate construction complexity.

---

# Fixtures

Fixtures provide reusable infrastructure required by tests.

Typical fixtures include:

- DuckLake catalog
- SQLModel session
- Temporary S3 bucket
- Repository instances
- Service instances
- Dependency Injection container
- Temporary configuration
- Sample metadata

Fixtures manage infrastructure.

Factories generate data.

These responsibilities should remain separate.

---

# Test Data

Reusable datasets belong in:

```
tests/data/
```

Datasets should be:

- Small
- Representative
- Deterministic
- Easy to understand

Avoid production datasets.

---

# Fake Data

The project uses **Faker** to generate realistic but deterministic test data.

Examples include:

- Series codes
- Countries
- Currencies
- Descriptions
- Vendor identifiers

Factories may internally use Faker.

Seed Faker whenever deterministic output is required.

---

# Validation Testing

Every validator requires dedicated tests.

Examples include:

- Valid input
- Missing fields
- Invalid values
- Incorrect data types
- Boundary conditions
- Empty datasets

Both **Pydantic** and **Pandera** schemas should have comprehensive test coverage.

---

# Repository Testing

Repositories should be tested against real infrastructure.

Repository tests should verify:

- Reads
- Writes
- Updates
- Deletes
- Transactions
- Constraints
- Query execution

Repositories should not be mocked.

---

# Service Testing

Services should verify business behaviour.

Tests should focus on:

- Business rules
- Validation
- Orchestration
- Error handling

Repository behaviour should already be covered by repository tests.

---

# QuerySet Testing

QuerySet tests should verify:

- Lazy evaluation
- Query chaining
- Filter composition
- Sorting
- Projection
- Execution methods
- Immutability

Creating a QuerySet should never trigger database access.

---

# Public API Testing

The public API should be tested as a user would interact with it.

Verify:

- Fluent interface
- Return types
- Error handling
- Documentation examples
- Backward compatibility
- Stable behaviour

---

# Vendor Adapter Testing

Every vendor adapter should verify:

- Successful responses
- Missing fields
- Invalid responses
- Empty responses
- Data normalization
- Validation

Vendor adapters should be tested independently of external vendor systems.

---

# Import Pipeline Testing

Metadata import tests should verify:

- CSV imports
- Excel imports
- Duplicate detection
- Rollbacks
- Validation failures
- Successful imports
- Audit logging

No invalid metadata should reach the production catalog.

---

# Error Handling Tests

Every custom exception should have corresponding tests.

Verify:

- Exception type
- Error message
- Error propagation
- Recovery behaviour

Infrastructure exceptions should be translated into domain exceptions.

---

# Performance Testing

Performance tests should focus on realistic workloads.

Examples include:

- Large metadata imports
- Bulk value retrieval
- Large QuerySets
- Metadata lookups
- DuckLake query execution

Performance tests should be repeatable.

---

# Regression Testing

Every production bug must result in a regression test.

A bug is not considered fixed until an automated test prevents it from reoccurring.

---

# Mocking Strategy

Mock only external systems.

Examples include:

- Vendor APIs
- Authentication
- Network failures
- Time
- External services

Do not mock:

- Services
- Repositories
- Validators
- Business logic

Prefer real implementations whenever practical.

Use factories instead of mocks for generating test data.

---

# Testing Infrastructure

Integration tests should execute against real infrastructure.

Recommended tools:

| Purpose | Tool |
|----------|------|
| Test Runner | pytest |
| Assertions | pytest |
| Coverage | pytest-cov |
| Mocking | pytest-mock |
| Fake Data | Faker |
| Containers | Testcontainers |
| Object Storage | LocalStack or MinIO |
| Dependency Injection | dependency-injector |
| Validation | Pandera |

DuckLake integration tests should use temporary DuckLake catalogs and isolated object storage.

Tests should never depend on shared development environments.

---

# Continuous Integration

Every pull request should automatically execute:

- Ruff
- BasedPyright
- Unit Tests
- Integration Tests

End-to-end and performance tests may execute in dedicated CI workflows.

No pull request should be merged with failing quality checks.

---

# Code Coverage

Code coverage measures confidence, not quality.

Target coverage:

- Overall: **≥ 90%**
- Business Logic: **≈ 100%**
- Critical Components: **100%**

Coverage targets should never encourage meaningless tests.

---

# Test Review Checklist

Before merging, verify:

- Tests are deterministic.
- Tests are readable.
- Factories are reused.
- Fixtures are reused.
- No duplicated setup exists.
- Validation is tested.
- Error handling is tested.
- Logging behaviour is verified where appropriate.
- Documentation examples remain valid.

---

# Example Test

Tests should read like business scenarios.

```python
metadata = MetadataFactory.create(
    currency="USD",
    asset_class="Rates",
)

curve = TimeSeriesFactory.usd_rates()

repository.save(curve)

result = (
    api
    .get(asset_class="Rates", currency="USD")
    .value()
)

assert result.height == curve.height
```

A reader should understand the business intent without needing to know how the platform is implemented.

---

# Summary

Testing is a core engineering practice of the MQL Datalake API.

The platform emphasizes:

- Behaviour-driven testing
- Domain-driven test factories
- Real infrastructure integration
- Deterministic execution
- Comprehensive validation
- High confidence through automation

Well-designed tests should be expressive, reusable, and closely aligned with the financial concepts exposed by the public API.

---

# Next Reading

- 08_logging_observability.md
- 09_vendor_integration.md
- 10_metadata_management.md
```