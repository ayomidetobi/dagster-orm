# MQL Datalake API

> **Document:** Database Design
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

This document defines the persistence architecture of the MQL Datalake API.

The platform uses **DuckLake** as its storage engine and catalog. DuckLake manages metadata, transactions, snapshots, and table definitions while storing data in object storage.

Application code interacts exclusively with DuckLake.

PostgreSQL is used internally by DuckLake as its catalog backend and should never be accessed directly by application code.

---

# Design Philosophy

The persistence layer should satisfy the following principles:

- One logical database.
- One query engine.
- Strong transactional guarantees.
- Versioned datasets.
- Object storage as the source of truth.
- Storage implementation hidden from the application.

Business logic should never know where data is physically stored.

---

# Database Architecture

```text
                   DataAPI
                      │
                  QuerySet
                      │
                   Services
                      │
                Repositories
                      │
                  DuckLake API
                      │
         ┌────────────┴────────────┐
         │                         │
 DuckLake Catalog           Object Storage
(PostgreSQL Backend)            (S3)
```

Application code communicates only with DuckLake.

DuckLake coordinates all catalog operations and object storage access.

---

# Why DuckLake?

DuckLake provides capabilities required by a financial data platform:

- ACID transactions
- Snapshot isolation
- Table versioning
- Schema evolution
- Object storage integration
- SQL compatibility
- Efficient analytical queries

This allows MQL to manage financial datasets without maintaining separate storage and metadata systems.

---

# Role of PostgreSQL

PostgreSQL is **not** an application database.

Its responsibilities are limited to supporting the DuckLake catalog.

Application code must never:

- open PostgreSQL sessions
- execute SQL against PostgreSQL
- define SQLModel models for catalog tables
- access catalog tables directly

All catalog operations occur through DuckLake.

---

# Role of Object Storage

Object storage contains the actual datasets.

Examples include:

- metadata tables
- historical values
- derived datasets
- imported datasets

DuckLake determines how and where objects are stored.

Application code should never construct object storage paths manually.

---

# Persistence Layers

```
Services

↓

Repositories

↓

DuckLake

↓

Object Storage
```

Repositories are the only layer responsible for persistence.

---

# Repository Pattern

Every table should have a repository.

Examples include:

- MetadataRepository
- ValueRepository
- ImportRepository

Repositories are responsible for:

- CRUD operations
- query execution
- transaction boundaries

Repositories should never implement business rules.

---

# Transactions

DuckLake provides transactional guarantees.

Repositories should ensure that related operations occur within the same transaction.

Examples include:

- metadata import
- metadata updates
- bulk inserts

Partial writes should never leave the catalog in an inconsistent state.

---

# Table Design

Tables should model business concepts rather than storage concerns.

Examples include:

- metadata
- values
- imports
- audit_logs

Avoid creating tables that duplicate derived information.

---

# Metadata Table

The metadata table is the semantic catalog of the platform.

Responsibilities include:

- dataset discovery
- vendor mappings
- business classifications
- series identifiers

Metadata should never contain financial observations.

---

# Values Table

The values table stores financial observations.

Typical fields include:

- series_code
- timestamp
- value

The values table should not duplicate metadata fields.

Metadata is resolved through joins when required.

---

# Primary Keys

Every table should define a stable primary key.

Examples:

Metadata

- series_code

Values

- (series_code, timestamp)

Primary keys should be immutable.

---

# Constraints

Define constraints at the database level whenever practical.

Examples include:

- primary keys
- unique constraints
- foreign keys
- not-null constraints

Business validation should occur before persistence.

---

# Schema Evolution

DuckLake supports schema evolution.

Schema changes should be:

- backward compatible when possible
- documented
- tested

Breaking schema changes should require explicit migration planning.

---

# Migrations

Schema evolution should be managed through Alembic.

Alembic is responsible for:

- creating tables
- modifying schemas
- adding indexes
- constraint changes

Migration files should be committed to version control.

Application startup should never modify schemas automatically.

---

# SQLModel

SQLModel defines the application's persistence models.

Responsibilities include:

- table definitions
- relationships
- constraints
- type annotations

Business logic should not exist inside SQLModel models.

Models should represent data only.

---

# DuckLake Access

Repositories should communicate with DuckLake through a shared connection provider.

Repositories should never create their own connections.

Connection management should be handled through dependency injection.

---

# Validation

Validation occurs before persistence.

Pipeline:

```
Pydantic

↓

Pandera

↓

Repository

↓

DuckLake
```

Only validated data should reach the persistence layer.

---

# Versioning

DuckLake snapshots provide built-in dataset versioning.

The platform should rely on DuckLake snapshots rather than implementing custom versioning logic.

---

# Performance

Repositories should optimize:

- predicate pushdown
- projection
- partition pruning
- lazy execution

Business logic should never optimize SQL manually.

---

# Testing

Repository tests should execute against a temporary DuckLake catalog.

Tests should verify:

- inserts
- updates
- deletes
- transactions
- constraints
- rollback behaviour

Mock repositories should be avoided whenever practical.

---

# Anti-Patterns

Avoid:

- direct PostgreSQL access
- manual object storage paths
- SQL outside repositories
- business logic inside models
- repositories calling other repositories
- persistence inside services

DuckLake should remain the single persistence interface.

---

# Summary

DuckLake is the persistence layer of the MQL Datalake API.

It provides transactional storage, catalog management, schema evolution, and versioning while storing datasets in object storage.

Application code remains independent of storage implementation by interacting only with repositories and DuckLake.

---

# Related Documents

- 02_architecture.md
- 05_repository_pattern.md
- 10_metadata_management.md
- 