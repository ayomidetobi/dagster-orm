# MQL Datalake API

> **Document:** Metadata Management
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

Metadata is the semantic foundation of the MQL Datalake API.

Every dataset stored within the platform is described by metadata, allowing users to discover and query financial data using business concepts instead of vendor-specific identifiers.

This document defines how metadata is structured, validated, imported, queried, and maintained.

---

# Design Philosophy

Metadata should describe **what** a dataset represents rather than **where** it came from.

Users should search using concepts such as:

- Asset Class
- Product
- Currency
- Country
- Tenor
- Region
- Frequency

rather than:

- Bloomberg ticker
- Bloomberg field
- MDS ticker
- Vendor identifiers

Metadata provides the semantic abstraction layer for the platform.

---

# Metadata Lifecycle

Every metadata record follows the same lifecycle.

```text
CSV / Excel

↓

Validation

↓

Transformation

↓

Business Validation

↓

DuckLake Catalog

↓

Query

↓

Data Retrieval
```

Every metadata record should follow this workflow before becoming available to users.

---

# Metadata Responsibilities

Metadata is responsible for:

- dataset discovery
- semantic querying
- vendor mappings
- series identification
- data classification
- business context

Metadata should never contain financial values.

Time-series values are stored separately.

---

# Metadata Model

Each metadata record describes a single financial series.

Typical fields include:

## Business Fields

- asset_class
- product
- sub_product
- country
- region
- currency
- tenor
- frequency
- description

## Technical Fields

- series_code
- source
- status
- created_at
- updated_at

## Vendor Mapping

- bloomberg_ticker
- bloomberg_field
- mds_ticker
- mds_field

Vendor mappings are implementation details and should not be exposed through the public API.

---

# Metadata Storage

Metadata is stored in DuckLake using SQLModel models.

DuckLake manages:

- catalog
- schema
- transactions
- versioning

Application code should not manipulate metadata tables directly.

All access should occur through repositories.

---

# Metadata Repository

The MetadataRepository owns persistence.

Responsibilities include:

- create
- update
- delete
- query
- search

Repositories should not implement business rules.

---

# Metadata Services

Business rules belong in the MetadataService.

Responsibilities include:

- validation
- duplicate detection
- import orchestration
- export orchestration
- metadata updates

Services coordinate repositories but remain independent of storage implementation.

---

# Metadata Import

The platform supports importing metadata from:

- CSV
- Excel

Imports should follow a consistent pipeline.

```text
File

↓

Pydantic Validation

↓

Pandera Validation

↓

Transformation

↓

Business Validation

↓

Repository

↓

DuckLake
```

Every stage must succeed before persistence.

---

# Metadata Export

Metadata may be exported to:

- CSV
- Excel
- Polars DataFrame

Exports should always use the latest committed metadata.

---

# Validation

Metadata validation occurs at multiple levels.

## File Validation

Verify:

- supported format
- required sheets
- required columns

## Schema Validation

Implemented using Pandera.

Verify:

- column names
- data types
- nullability
- uniqueness

## Business Validation

Verify:

- unique series codes
- valid currencies
- valid frequencies
- valid asset classes
- vendor mappings

---

# Series Code

The `series_code` is the canonical identifier within MQL.

All relationships should use the series code rather than vendor identifiers.

Series codes must be:

- unique
- immutable
- human-readable where practical

Changing a series code should be considered a breaking change.

---

# Vendor Mapping

Metadata maps semantic financial concepts to vendor identifiers.

Example:

```text
Series Code

↓

Bloomberg Ticker

↓

Bloomberg Field

↓

MDS Ticker

↓

MDS Field
```

Vendor mappings are internal implementation details.

---

# Querying Metadata

Metadata queries should use semantic fields.

Example

```python
api.get(
    asset_class="Rates",
    currency="USD",
    tenor="10Y",
)
```

The query engine resolves matching series codes using metadata before retrieving values.

---

# Metadata Updates

Metadata updates should be:

- validated
- transactional
- logged
- auditable

Partial updates should not leave the catalog in an inconsistent state.

---

# Duplicate Detection

The platform should prevent duplicate metadata.

Duplicates may include:

- duplicate series codes
- duplicate vendor mappings
- conflicting business attributes

Validation should occur before persistence.

---

# Auditing

Metadata changes should be auditable.

Important events include:

- imports
- updates
- deletions
- schema changes

Audit information should include:

- timestamp
- operation
- affected records

---

# Versioning

DuckLake provides table versioning.

The platform should rely on DuckLake snapshots for metadata recovery rather than implementing custom versioning logic.

---

# Metadata Cache

Metadata changes infrequently compared to financial values.

Metadata may be cached to improve query performance.

Suitable cache candidates include:

- lookup tables
- series mappings
- filter indexes

Cache invalidation should occur automatically after metadata changes.

---

# Testing

Metadata requires comprehensive testing.

Tests should verify:

- imports
- exports
- validation
- duplicate detection
- updates
- repository behaviour
- service behaviour

Factories should generate representative metadata records.

---

# Anti-Patterns

Avoid:

- querying values without metadata
- exposing vendor identifiers publicly
- bypassing validation
- duplicate series codes
- business logic in repositories
- direct SQL outside repositories

Metadata should remain the single source of truth for dataset discovery.

---

# Summary

Metadata is the semantic layer of the MQL Datalake API.

It enables users to discover financial datasets using business concepts while isolating the rest of the platform from vendor-specific implementation details.

Correct metadata is essential for reliable querying, ingestion, and long-term maintainability.

---

# Next Reading

- 11_project_conventions.md
- 12_error_handling.md