# MQL Datalake API

> **Document:** Logging & Observability
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Software Engineers, Platform Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the logging, monitoring, and observability standards for the MQL Datalake API.

Observability enables engineers to understand the health, behaviour, and performance of the platform in production.

Logging is not only for debugging—it is also essential for monitoring, auditing, performance analysis, and troubleshooting.

---

# Observability Philosophy

Every important operation should answer the following questions:

- What happened?
- When did it happen?
- Why did it happen?
- How long did it take?
- What data was affected?
- Did it succeed?
- If it failed, why?

Logs should make it possible to understand the complete lifecycle of a request without reproducing the issue.

---

# Pillars of Observability

The platform uses three pillars of observability.

```
Logs

↓

Metrics

↓

Tracing
```

Each provides different information about the platform.

---

# Structured Logging

The project uses **structlog** exclusively.

Do not use:

```python
print(...)
```

or

```python
logging.getLogger(...)
```

All application logging should use a shared logger.

Example

```python
logger.info(
    "metadata_import_completed",
    file_name="rates.xlsx",
    rows=2450,
    duration_ms=812,
)
```

Logs should be machine-readable and human-readable.

---

# Logger Creation

Create loggers using:

```python
logger = structlog.get_logger(__name__)
```

Never create custom logging wrappers.

The dependency injection container should configure logging during application startup.

---

# Log Levels

Use log levels consistently.

| Level | Purpose |
|---------|----------|
| DEBUG | Internal development information |
| INFO | Normal business events |
| WARNING | Unexpected but recoverable situations |
| ERROR | Failed operations |
| CRITICAL | System-wide failures |

Do not misuse ERROR for expected validation failures.

---

# Log Structure

Every log entry should contain contextual information whenever available.

Examples include:

- request_id
- operation
- duration_ms
- repository
- service
- vendor
- series_code
- row_count
- file_name

Structured logs are preferred over formatted strings.

Good

```python
logger.info(
    "query_completed",
    duration_ms=120,
    row_count=8000,
)
```

Avoid

```python
logger.info(
    f"Query completed in {duration}"
)
```

---

# Request Correlation

Every request should have a unique request identifier.

The request identifier should be propagated across all services and repositories.

This enables tracing a request throughout the platform.

---

# Business Events

Important business events should always be logged.

Examples include:

- Metadata import started
- Metadata import completed
- Vendor synchronization
- Query execution
- Validation failure
- Cache refresh
- Snapshot creation

---

# Error Logging

Log errors with context.

Example

```python
logger.exception(
    "vendor_import_failed",
    vendor="Bloomberg",
    series_code="US10Y",
)
```

Avoid logging stack traces manually.

Use `logger.exception()` for unexpected exceptions.

---

# Validation Logging

Validation failures should include:

- validator
- failed fields
- row number (when applicable)
- reason

Validation failures should not be logged as system errors unless they indicate a platform defect.

---

# Performance Logging

Measure significant operations.

Examples include:

- Metadata import duration
- DuckLake query duration
- Repository execution time
- Vendor API latency
- Cache refresh duration

Performance metrics should be logged automatically.

---

# Metrics

Metrics provide long-term operational visibility.

Examples include:

- Query count
- Import count
- Import duration
- Failed imports
- Cache hits
- Cache misses
- Vendor latency
- Rows imported
- Rows queried

Metrics should be aggregated rather than stored as logs.

---

# Tracing

Tracing follows a request through multiple components.

Example

```
API

↓

QuerySet

↓

Service

↓

Repository

↓

DuckLake

↓

S3
```

Every step should preserve the request identifier.

---

# Dependency Injection

The logger should be provided through the dependency injection container.

Application components should never configure logging themselves.

Logging configuration should occur once during application startup.

---

# Sensitive Data

Never log:

- Credentials
- API keys
- Access tokens
- Database passwords
- Connection strings

Log metadata only when necessary and appropriate.

---

# Audit Logging

Certain business operations require audit logs.

Examples include:

- Metadata imports
- Metadata updates
- Metadata deletion
- Configuration changes

Audit logs should include:

- Timestamp
- User (when available)
- Operation
- Resource
- Outcome

Audit logs should never be modified after creation.

---

# Monitoring

Production monitoring should include:

- Error rate
- Query latency
- Vendor failures
- Import failures
- DuckLake availability
- PostgreSQL catalog health
- Object storage availability

Alerts should be actionable.

---

# Exception Reporting

Unexpected exceptions should include:

- Exception type
- Request identifier
- Context
- Stack trace

Avoid swallowing exceptions.

Unexpected failures should always be observable.

---

# Logging Checklist

Before merging code, verify:

- Meaningful logs are present.
- Structured logging is used.
- No sensitive information is logged.
- Contextual fields are included.
- Long-running operations record duration.
- Errors include sufficient context.
- Request identifiers are propagated.

---

# Anti-Patterns

Avoid:

- print()
- String-formatted logs
- Duplicate logs
- Missing context
- Logging sensitive data
- Logging inside tight loops
- Swallowing exceptions without logging

---

# Summary

Observability is a fundamental part of the MQL Datalake API.

Every operation should be traceable, measurable, and diagnosable.

Structured logging, meaningful metrics, and request tracing enable the platform to remain reliable, maintainable, and easy to operate in production.

---

# Next Reading

- 09_vendor_integration.md
- 10_metadata_management.md