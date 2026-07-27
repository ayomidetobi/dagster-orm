"""Rewrite package for the DuckLake-based data API."""

__all__ = [
    "DataAPI",
    "QuerySet",
    "RewriteContainer",
    "build_rewrite_container",
    "create_container",
    "create_data_api",
    "configure_logging",
]

_EXPORTS = {
    "DataAPI": "rewrite.data_api.api.data_api",
    "QuerySet": "rewrite.data_api.api.queryset",
    "RewriteContainer": "rewrite.data_api.container",
    "build_rewrite_container": "rewrite.data_api.container",
    "create_container": "rewrite.data_api.factory",
    "create_data_api": "rewrite.data_api.factory",
    "configure_logging": "rewrite.data_api.logging",
}


def __getattr__(name: str) -> object:
    """Import exports lazily.

    resources/duckdb_datacacher.py imports rewrite.data_api.models.config,
    while this package's repositories import back from
    resources/duckdb_datacacher.py. Eagerly importing everything here would
    make that a circular import; resolving on first attribute access breaks
    the cycle without changing the public `from dagster_quickstart.rewrite import DataAPI` shape.
    """
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(module_name)
    return getattr(module, name)
