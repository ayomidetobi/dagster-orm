# MQL Datalake API

> **Document:** API Design
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Quant Researchers, Software Engineers, Data Engineers
>
> **Last Updated:** 2026-07-15

---

# Purpose

This document defines the public API of the MQL Datalake API.

The API is designed around semantic financial concepts rather than vendors, databases, storage engines, or SQL.

Consumers should express **what** they want, not **how** to retrieve it.

---

# Design Philosophy

The API is designed around five principles.

- Semantic
- Fluent
- Lazy
- Predictable
- Stable

Users should think in terms of financial concepts.

Example

```python
api.get(
    asset_class="Rates",
    currency="USD",
    tenor="10Y",
).value()
```

Instead of

```python
ducklake.execute(...)
```

or

```python
SELECT * FROM ...
```

---

# Public Entry Point

Every interaction begins with the `DataAPI`.

```python
from mql import DataAPI

api = DataAPI()
```

Consumers should never instantiate repositories, services, or infrastructure components directly.

---

# Fluent Interface

The API follows a fluent interface.

Example

```python
(
    api
    .get(asset_class="Rates")
    .filter(currency="USD")
    .sort("tenor")
    .value()
)
```

Each method returns a new QuerySet until execution is requested.

---

# Lazy Evaluation

QuerySets are lazy.

Creating a QuerySet does **not** execute a query.

Example

```python
query = (
    api
    .get(currency="USD")
    .filter(asset_class="Rates")
)
```

No database query has occurred.

Execution happens only when calling methods such as:

- `.value()`
- `.info()`
- `.count()`
- `.exists()`
- `.first()`
- `.last()`

---

# Immutable QuerySets

QuerySets should be immutable.

Example

```python
rates = api.get(asset_class="Rates")

usd = rates.filter(currency="USD")

eur = rates.filter(currency="EUR")
```

The original QuerySet remains unchanged.

---

# Semantic Filtering

Filters should use business terminology.

Good

```python
api.get(
    asset_class="FX",
    currency="EUR",
    country="Germany",
)
```

Avoid exposing vendor-specific identifiers.

Bad

```python
api.get(
    bloomberg_ticker="..."
)
```

---

# Public API Methods

The public interface should remain intentionally small.

## DataAPI

```python
get()

get_values()

get_last_value()

import_metadata()

export_metadata()

refresh_metadata()
```

---

## QuerySet

```python
filter()

exclude()

select()

sort()


value()

info()

count()

exists()

first()

last()

to_polars()
```

Every new public method should have a clear semantic purpose.

---

# Return Types

Methods should always return predictable types.

| Method | Returns |
|---------|----------|
| value() | Polars DataFrame |
| info() | Polars DataFrame |
| count() | int |
| exists() | bool |
| first() | Polars DataFrame |
| last() | Polars DataFrame |

Methods should never return different types depending on input.

---

# Method Naming

Method names should describe user intent.

Examples

```python
value()

info()

count()

exists()
```

Avoid names tied to implementation.

Bad

```python
execute_sql()

read_ducklake()

fetch_parquet()
```

---

# Method Responsibilities

Each public method should have one responsibility.

Good

```python
value()
```

Returns values.

Good

```python
info()
```

Returns metadata.

Avoid methods that perform multiple unrelated operations.

---

# Chaining

Query methods should be chainable.

Example

```python
(
    api
    .get(asset_class="FX")
    .filter(currency="USD")
    .sort("country")
    .info()
)
```

Execution methods terminate the chain.

---

# Error Handling

The public API should expose meaningful exceptions.

Example

```python
try:
    api.get(...).value()
except QueryError:
    ...
```

Infrastructure exceptions should never be exposed directly.

---

# Validation

Arguments should be validated before execution.

Invalid queries should fail immediately with descriptive error messages.

---

# Performance

The API should minimise unnecessary work.

Examples

- Lazy evaluation
- Predicate pushdown
- Projection pushdown
- Vectorised execution

Consumers should not need to optimise queries manually.

---

# Stability

The public API is considered stable.

Internal implementation may change without affecting users.

Breaking API changes require:

- documentation updates
- migration guide
- version increment

---

# Extensibility

New capabilities should integrate naturally into the existing fluent interface.

Example

```python
(
    api
    .get(...)
    .group_by(...)
    .aggregate(...)
    .value()
)
```

rather than introducing separate APIs.

---

# Examples

## Retrieve metadata

```python
(
    api
    .get(currency="USD")
    .info()
)
```

---

## Retrieve values

```python
(
    api
    .get(
        asset_class="Rates",
        tenor="10Y",
    )
    .value()
)
```

---

## Check existence

```python
(
    api
    .get(country="France")
    .exists()
)
```

---

## Retrieve latest observation

```python
(
    api
    .get(series_code="US10Y")
    .last()
)
```

---

# Anti-Patterns

Avoid exposing:

- SQL
- DuckLake
- PostgreSQL
- S3
- Vendor identifiers
- Internal repositories

Consumers should interact only with financial concepts.

---

# API Evolution

When introducing a new public method:

- Follow existing naming conventions.
- Preserve fluent chaining.
- Return a predictable type.
- Add documentation.
- Add usage examples.
- Add unit and integration tests.

The API should evolve incrementally while remaining intuitive.

---

# Summary

The MQL Datalake API provides a semantic, fluent, and stable interface for accessing financial market data.

Consumers interact with financial concepts rather than implementation details, allowing the platform to evolve internally while maintaining a consistent developer experience.

---

# Next Reading

- 06_development_guide.md
- 07_testing_strategy.md