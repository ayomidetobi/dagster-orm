"""STEER-style FX fair-value model layer.

Pure business logic (config, feature engineering, OLS/cointegration
estimation, signal generation, gold-layer storage) with no Dagster
dependency -- see dagster_quickstart/assets/steer/ for the thin Dagster
asset wiring on top, mirroring how rewrite/data_api/ holds the DuckLake
business logic that assets/ingestion/ wires into assets.
"""
