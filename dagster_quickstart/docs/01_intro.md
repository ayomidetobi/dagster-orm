# MQL Datalake API

> **Document:** Introduction
>
> **Version:** 1.0
>
> **Status:** Draft
>
> **Audience:** Quant Researchers, Data Engineers, Software Engineers, Platform Engineers
>
> **Last Updated:** 2026-07-15

---

# Introduction

## Vision

The **MQL (Macro Quant Library) Datalake API** is a semantic financial data platform designed to provide the M360 Macro Quant team with a single, consistent, and reliable interface for accessing financial market data.

The platform abstracts market data vendors, storage technologies, and infrastructure concerns behind a unified Python API, allowing researchers and applications to interact with financial concepts rather than implementation details.

Whether data originates from Bloomberg, MDS, Hawk, or future data providers, consumers interact with a consistent interface without needing knowledge of vendor-specific APIs, licensing requirements, or storage mechanisms.

The platform serves as the central source of market data for the M360 team, enabling quantitative research, model development, and production systems to consume governed financial datasets through a single library.

---

# Mission

The mission of the MQL Datalake API is to provide a modern, scalable, and maintainable data platform that:

- Centralizes financial market data for the M360 Macro Quant team.
- Provides a semantic API for querying financial datasets.
- Abstracts vendor-specific implementations behind a common interface.
- Abstracts storage implementation from application logic.
- Enables internal quantitative models to consume governed financial data without directly integrating with external market data providers.
- Provides a foundation for scalable data ingestion, storage, and analytics.

The platform is designed so that consumers interact only with business concepts while the platform transparently resolves vendor mappings, metadata, storage locations, and retrieval strategies.

---

# Scope

The MQL Datalake API is a Python library responsible for providing access to financial market data through a semantic query interface.

The platform is responsible for:

- Metadata management
- Financial data retrieval
- Vendor abstraction
- Storage abstraction
- Metadata validation
- Data validation
- Time-series querying
- Metadata import and export
- Data ingestion interfaces
- Internal data governance

The platform is **not** responsible for:

- Financial modelling
- Forecast generation
- Trading execution
- Portfolio management
- Risk calculations
- Data visualization

These responsibilities belong to applications built on top of the library.

---

# Target Audience

The primary users of the platform are:

- Quantitative Researchers
- Data Engineers
- Platform Engineers
- Internal Python applications
- Quantitative models running within the M360 environment

---

# Core Principles

The architecture is built upon several fundamental principles.

## Semantic First

Consumers interact with financial concepts such as asset classes, currencies, countries, tenors, products, and market sectors rather than vendor identifiers or storage details.

Example:

```python
api.get(
    asset_class="Rates",
    currency="USD",
    tenor="10Y"
).value()
```

instead of

```python
fetch_vendor_data(
    ticker="USGG10YR Index",
    vendor="Bloomberg"
)
```

---

## Vendor Agnostic

The source of market data is considered an implementation detail.

Consumers should never need to know whether data originates from Bloomberg, MDS, Hawk, or any future provider.

---

## Storage Agnostic

Consumers should never know whether data is stored in DuckLake, PostgreSQL, or another storage technology.

Storage decisions remain internal implementation details.

---

## Single Source of Truth

The platform acts as the governed source of financial market data for the M360 Macro Quant team.

All consumers retrieve data through the same interface using the same metadata definitions.

---

## Extensible by Design

New vendors, metadata fields, storage engines, and query capabilities should be introduced without changing the public API.

---

# Platform Capabilities

The MQL Datalake API provides capabilities including:

- Semantic financial data queries
- Time-series retrieval
- Metadata discovery
- Metadata filtering
- Vendor abstraction
- Metadata import
- Metadata export
- Centralized metadata management
- High-performance analytical queries
- Scalable data ingestion
- Data validation
- Metadata validation

---

# High-Level Architecture

```
                    Client

                       │

                  DataAPI

                       │

                  QuerySet

                       │

                  Services

                       │

                Repositories

               ┌────────┴────────┐

               │                 │

        PostgreSQL         DuckLake

        (Metadata)      (Time-Series)

               │                 │

               └────────┬────────┘

                        │

               Market Data Vendors

         Bloomberg • MDS • Hawk • ...
```

---

# Technology Stack

| Layer | Technology |
|--------|------------|
| Programming Language | Python |
| Metadata Database | PostgreSQL |
| Time-Series Storage | DuckLake |
| Object Storage | Amazon S3 |
| ORM | SQLModel |
| Database Migration | Alembic |
| Data Validation | Pandera |
| Object Validation | Pydantic |
| Data Processing | Polars |
| Logging | structlog |
| Dependency Injection | dependency-injector |
| Workflow Orchestration | Dagster |
| Testing | pytest |

---

# Design Philosophy

The MQL Datalake API is designed around the principle that financial data access should be expressed in terms of business concepts rather than infrastructure details.

Consumers should never need to understand:

- Vendor APIs
- Vendor-specific identifiers
- Database schemas
- Storage layouts
- S3 paths
- DuckLake internals
- Metadata relationships

Instead, the platform resolves these implementation details internally, providing a consistent, intuitive interface for financial data access.

This separation enables the platform to evolve internally while maintaining a stable public API for all consumers.

---

# Terminology

| Term | Description |
|------|-------------|
| DataAPI | Primary entry point into the platform. |
| QuerySet | Represents a semantic query over metadata and values. |
| Metadata | Descriptive information defining financial series. |
| Series | A uniquely identifiable financial time series. |
| Vendor | External market data provider such as Bloomberg or MDS. |
| DuckLake | Storage engine for time-series data. |
| PostgreSQL | Metadata and platform database. |

---

# Related Documentation

The following documents describe the architecture and implementation in greater detail:

- **02_architecture.md** — Overall system architecture and component responsibilities.
- **03_engineering_standards.md** — Coding standards and engineering principles.
- **04_development_guide.md** — Development workflows and extension points.
- **05_database_design.md** — PostgreSQL and DuckLake schema design.
- **06_api_design.md** — Public API design and conventions.
- **07_testing_strategy.md** — Testing philosophy and best practices.
- **08_logging_observability.md** — Logging, monitoring, and observability.
- **09_vendor_integration.md** — Vendor integration architecture.
- **10_metadata_management.md** — Metadata lifecycle and governance.